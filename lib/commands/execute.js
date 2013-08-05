var Command  = require('./command');
var Packets  = require('../packets/index.js');
var util    = require('util');
var compileParser = require('../compile_binary_parser');
var ServerStatus = require('../constants/server_status.js');

/*
  flow of commands/data:
  execute(sql, params) ->
    if have cached PS for sql ->
      do_execute
    prepare(sql) ->
      prepareHeader -> numFieldDefs, numParamDefs
      readFields
      readParams
      save PS to cache (ps id, fields defs, params defs)
      if fields not empty
        compileRowParser(fieldDefs)
        save to parsers cache (key is fields names+types+flags)
      do_execute

   do_execute
      send CMD_EXECYRE + stmtId
      read header -> numFields
        if numFields = 0
          push header to _rows
          if (moreResults) ->
            go to read header
          else
            callback(_rows[0] if

 */



function Execute(sql, parameters, callback)
{
  Command.call(this);
  this.query = sql;
  this.parameters = parameters;

  this.onResult = callback;

  this.fieldCount = 0;
  this.parameterCount = 0;

  this._parameterDefinitions = [];
  this._resultFields = [];
  this._resultFieldCount = 0;
  this._rows = [];
  this._fields = [];
  this._statementInfo = null;
  this._resultIndex = 0;
}
util.inherits(Execute, Command);

Execute.prototype.start = function(packet, connection) {
  var cachedStatement = connection.statements[this.query];
  if (!cachedStatement) { // prepare first
    connection.writePacket(new Packets.PrepareStatement(this.query).toPacket(1));
  } else {
    this._statementInfo = cachedStatement;
    return this.doExecute(connection);
  }
  return Execute.prototype.prepareHeader;
};

function PreparedStatementInfo(id) {
  this.id = id;
  this.parser = null;
}

Execute.prototype.done = function() {
  if (this.onResult) {
    if (this._resultIndex === 0) {
      this.onResult(null, this._rows[0], this._fields[0], this._resultIndex+1);
    } else {
      this.onResult(null, this._rows, this._fields, this._resultIndex+1);
    }
  }
  return null;
};

Execute.prototype.prepareHeader = function(packet, connection) {
  var header = new Packets.PreparedStatementHeader(packet);
  this.fieldCount     = header.fieldCount;
  this.parameterCount = header.parameterCount;
  this._statementInfo = new PreparedStatementInfo(header.id);
  connection.statements[this.query] = this.statementInfo;
  if (this.parameterCount > 0)
    return Execute.prototype.readParameter;
  else if (this.fieldCount > 0)
    return Execute.prototype.readField;
  else
    return this.doExecute(connection);
};

Execute.prototype.readParameter = function(packet, connection) {
  var def = new Packets.ColumnDefinition(packet);
  this._parameterDefinitions.push(def);
  if (this._parameterDefinitions.length == this.parameterCount)
    return Execute.prototype.parametersEOF;
  return this.readParameter;
};

// TODO: move to connection.js?
function getFieldsKey(fields) {
  var res = '';
  for (var i=0; i < fields.length; ++i)
    res += '/' + fields[i].name + ':' + fields[i].columnType + ':' + fields[i].flags;
  return res;
}

Execute.prototype.readField = function(packet, connection) {
  var def = new Packets.ColumnDefinition(packet);
  this.fields.push(def);

  // TODO: api to allow to flag "I'm not going to change schema for this statement"
  // this way we can ignore column definitions in binary response and use
  // definition from prepare phase. Note that it's what happens currently
  // e.i if you do execute("select * from foo") and later add/remove/rename rows to foo
  // (without reconnecting) you are in trouble

  if (this.fields.length == this.fieldCount) {
    // compile row parser
    var parserKey = getFieldsKey(this.fields);
    // try cached first
    this._statementInfo.fields = this.fields;
    this._statementInfo.parser = connection.binaryProtocolParsers[parserKey];
    if (!this._statementInfo.parser) {
      this._statementInfo.parser = compileParser(this.fields);
      connection.binaryProtocolParsers[parserKey] = this.statementInfo.parser;
    }
    return Execute.prototype.fieldsEOF;
  }
  return Execute.prototype.readField;
};

Execute.prototype.parametersEOF = function(packet, connection) {
  // check EOF
  if (!packet.isEOF())
    throw "Expected EOF packet";
  if (this.fieldCount > 0)
    return Execute.prototype.readField;
  else
    return this.doExecute(connection);
};

Execute.prototype.fieldsEOF = function(packet, connection) {
  // check EOF
  if (!packet.isEOF())
    throw "Expected EOF packet";
  return this.doExecute(connection);
};

Execute.prototype.doExecute = function(connection)
{
  connection.sequenceId = 0;
  var executePacket = new Packets.Execute(this.statementInfo.id, this.parameters);
  connection.writePacket(executePacket.toPacket(1));
  return Execute.prototype.resultesetHeader;
};

Execute.prototype.resultesetHeader = function(packet) {
  var rs = new Packets.ResultSetHeader(packet);
  this._resultFieldCount = rs.fieldCount;
  if (this._resultFieldCount === 0) {
    this._rows.push(rs);
    this._fields.push(void(0));
    this.emit('result', rs, this._resultIndex);
    this.emit('fields', void(0), this._resultIndex);
    if (rs.serverStatus & ServerStatus.SERVER_MORE_RESULTS_EXISTS) {
      this._resultIndex++;
      return Query.prototype.resultsetHeader;
    }
    return this.done();
  }
  return Execute.prototype.readResultField;
};

Execute.prototype.readResultField = function(packet, connection) {
  var def, parserKey;
  if (this.statementInfo.parser) // ignore result fields definition, we are reusing fields from prepare response
    this.resultFields.push(null);
  else {
    def = new Packets.ColumnDefinition(packet);
    this.resultFields.push(def);
  }
  if (this.resultFields.length == this.resultFieldCount) {
    if (this.statementInfo.parser)
    {
      this._parser = this.statementInfo.parser;
      this._fields = this.statementInfo.fields;
    } else {
      parserKey = getFieldsKey(this.resultFields);
      this._parser = connection.binaryProtocolParsers[parserKey];
      if (!this._parser) {
        this._parser = compileParser(this.resultFields);
        connection.binaryProtocolParsers[parserKey] = this._parser;
      }
      this._fields = this.resultFields;
    }
    return Execute.prototype.resultFieldsEOF;
  }
  return Execute.prototype.readResultField;
};

Execute.prototype.resultFieldsEOF = function(packet) {
  // check EOF
  if (!packet.isEOF())
    throw "Expected EOF packet";
  return Execute.prototype.row;
};

Execute.prototype.row = function(packet)
{
  // TODO: refactor to share code with Query::row
  if (packet.isEOF()) {
    var moreResults = packet.eofStatusFlags() & ServerStatus.SERVER_MORE_RESULTS_EXISTS;
    if (moreResults) {
      this._resultIndex++;
      return Execute.prototype.resultesetHeader;
    }
    if (this.onResult)
      this.onResult(null, this.rows, this._fields);
    return null;
  }

  var r = new this._parser(packet);
  if (this.onResult)
    this.rows.push(r);
  else
    this.emit('result', r);
  return Execute.prototype.row;
};

module.exports = Execute;
