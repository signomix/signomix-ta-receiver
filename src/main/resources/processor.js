/// default script
var ProcessorResult = Java.type("com.signomix.receiver.script.ProcessorResult");
var ProcessorResultHelper = Java.type("com.signomix.receiver.script.ProcessorResultHelper");
var ChannelData = Java.type("com.signomix.common.iot.ChannelData");

//deprecated
var result = new ProcessorResult()
var dataReceived = []
var channelReader = {}
//

var sgx0 = {}
sgx0.dataReceived = []
sgx0.result = new ProcessorResult()
sgx0.helper = new ProcessorResultHelper()
sgx0.dataTimestamp = 0
sgx0.channelReader = {}
sgx0.malformed = ''

sgx0.verify = function (received, receivedStatus) {
    this.dataReceived = []
    var tmpChannelData
    for (var i = 0; i < received.length; i++) {
        if (!(received[i] == null)) {
            tmpChannelData = new ChannelData(this.eui, received[i].name, received[i].value, received[i].timestamp);
            this.dataReceived.push(tmpChannelData)
            this.result.putData(tmpChannelData);
            this.result.log(tmpChannelData.toString());
        } else {
            malformed = received
        }
    }
    this.state = receivedStatus
}

sgx0.accept = function (name) {
    for (i = 0; i < this.dataReceived.length; i++) {
        if (this.dataReceived[i].getName() == name) {
            this.result.putData(this.eui, name, this.dataReceived[i].getValue(), this.dataReceived[i].getTimestamp());
        }
    }
}
sgx0.addCommand = function (targetEUI, payload, overwrite) {
    //JSON payload
    this.result.addCommand(targetEUI, this.eui, JSON.stringify(payload), 2, overwrite);
}
sgx0.addPlainCommand = function (targetEUI, payload, overwrite) {
    //TEXT payload
    this.result.addCommand(targetEUI, this.eui, payload, 0, overwrite);
}
sgx0.addHexCommand = function (targetEUI, payload, overwrite) {
    //for TTN devices payload must be String representing byte array as hex values
    //eg. 00FFAA01
    this.result.addCommand(targetEUI, this.eui, payload, 1, overwrite);
}
sgx0.addNotification = function (newType, newMessage) {
    //this.result.log(">>>>"+newType+">>"+newMessage+">>");
    this.result.addEvent(newType, newMessage);
}
sgx0.addVirtualData = function (newEUI, newName, newValue) {
    this.result.addDataEvent(newEUI, this.eui, new ChannelData(newEUI, newName, newValue, this.dataTimestamp));
}

sgx0.getAverage = function (channelName, scope, newValue) {
    if (isNaN(scope)) {
        throw new Error('scope is not a number');
    }
    if (scope < 1) {
        throw new Error('scope must be greater than 0');
    }
    if (newValue == undefined) {
        return this.channelReader.getAverageValue(channelName, scope).getValue();
    } else {
        if (isNaN(newValue)) {
            throw new Error('newValue is not a number');
        }
        return this.channelReader.getAverageValue(channelName, scope, newValue).getValue();
    }
}
sgx0.getMinimum = function (channelName, scope, newValue) {
    if (isNaN(scope)) {
        throw new Error('scope is not a number');
    }
    if (scope < 1) {
        throw new Error('scope must be greater than 0');
    }
    if (newValue == undefined) {
        return this.channelReader.getMinimalValue(channelName, scope).getValue();
    } else {
        if (isNaN(newValue)) {
            throw new Error('newValue is not a number');
        }
        return this.channelReader.getMinimalValue(channelName, scope, newValue).getValue();
    }
}
sgx0.getMaximum = function (channelName, scope, newValue) {
    if (isNaN(scope)) {
        throw new Error('scope is not a number');
    }
    if (scope < 1) {
        throw new Error('scope must be greater than 0');
    }
    if (newValue == undefined) {
        return this.channelReader.getMaximalValue(channelName, scope).getValue();
    } else {
        if (isNaN(newValue)) {
            throw new Error('newValue is not a number');
        }
        return this.channelReader.getMaximalValue(channelName, scope, newValue).getValue();
    }
}
sgx0.getSum = function (channelName, scope, newValue) {
    if (isNaN(scope)) {
        throw new Error('scope is not a number');
    }
    if (scope < 1) {
        throw new Error('scope must be greater than 0');
    }
    if (newValue == undefined) {
        return this.channelReader.getSummaryValue(channelName, scope).getValue();
    } else {
        if (isNaN(newValue)) {
            throw new Error('newValue is not a number');
        }
        return this.channelReader.getSummaryValue(channelName, scope, newValue).getValue();
    }
}
sgx0.getLastValue = function (channelName, skipNull) {
    var skipNullValues = false;
    if(arguments.length > 1){
        skipNullValues = skipNull;
    }
    var tmpLastData = this.channelReader.getLastData(channelName, skipNullValues);
    if (tmpLastData != null) {
        return tmpLastData.value
    } else {
        return null
    }
}
sgx0.getLastData = function (channelName, skipNull) {
    var skipNullValues = false;
    if(arguments.length > 1){
        skipNullValues = skipNull;
    }
    return this.channelReader.getLastData(channelName, skipNullValues);
}
sgx0.getModulo = function (value, divider) {
    return this.result.getModulo(value, divider);
}
sgx0.getOutput = function () {
    return this.result.getOutput();
}
sgx0.getTimestamp = function (channelName) {
    /* var ts = 0
    for (i = 0; i < this.dataReceived.length; i++) {
        if (this.dataReceived[i].getName() == channelName) {
            ts = this.dataReceived[i].getTimestamp()
            break
        }
    }
    if (ts == 0) ts = Date.now()
    return ts; */
    return this.dataTimestamp
}
sgx0.getTimestampUTC = function (y, m, d, h, min, s) {
    return Date.UTC(y, m - 1, d, h, min, s);
}
sgx0.getValue = function (channelName) {
    for (i = 0; i < this.dataReceived.length; i++) {
        if (this.dataReceived[i].getName() == channelName) {
            return this.dataReceived[i].getValue();
        }
    }
    return null;
}
sgx0.getStringValue = function (channelName) {
    for (i = 0; i < this.dataReceived.length; i++) {
        if (this.dataReceived[i].getName() == channelName) {
            return this.dataReceived[i].getStringValue();
        }
    }
    return null;
}
sgx0.getLastValue = function (channelName) {
    var tmpLastData = this.channelReader.getLastData(channelName);
    if (tmpLastData != null) {
        return tmpLastData.value
    } else {
        return null
    }
}
sgx0.getLastData = function (channelName) {
    return this.channelReader.getLastData(channelName);
}
sgx0.put = function (name, newValue, timestamp) {
    if (timestamp == undefined) {
        this.result.putData(this.eui, name, newValue, this.dataTimestamp);
    } else {
        this.result.putData(this.eui, name, newValue, timestamp);
    }
}

sgx0.setState = function (newState) {
    this.result.setDeviceStatus(newState);
}
sgx0.setStatus = function (newStatus) {
    this.result.setDeviceStatus(newStatus);
}

sgx0.reverseHex = function (hexStr) {
    if (!(typeof hexStr === 'string' || hexStr instanceof String)) {
        return 0
    }
    if (hexStr.length % 2 !== 0) {
        return 0
    }
    var result = ''
    for (i = hexStr.length - 2; i >= 0; i = i - 2) {
        result = result + hexStr.substring(i, i + 2)
    }
    return result
}
sgx0.swap32 = function (val) {
    return ((val & 0xFF) << 24)
        | ((val & 0xFF00) << 8)
        | ((val >> 8) & 0xFF00)
        | ((val >> 24) & 0xFF);
}
sgx0.distance = function (latitude1, longitude1, latitude2, longitude2) {
    return this.result.getDistance(latitude1, longitude1, latitude2, longitude2);
}
sgx0.xaddList = function (timestamp) {
    this.result.addDataList(timestamp);
}
sgx0.getTimeoffsetMinutes = function(timezoneName){
    return this.timeOffsets[timezoneName];
}

var processData = function (eui, dataReceived, channelReader, userID, receivedDataTimestamp,
    latitude, longitude, altitude, status, alert,
    devLatitude, devLongitude, devAltitude, newCommand, requestData, devConfig, appConfig, timeOffsets) {
    var ChannelData = Java.type("com.signomix.common.iot.ChannelData");
    var IotEvent = Java.type("com.signomix.common.event.IotEvent");
    var ProcessorResult = Java.type("com.signomix.receiver.script.ProcessorResult");
    var channelData = {};

    var sgx = Object.create(sgx0)
    sgx.eui = eui
    sgx.latitude = latitude
    if (sgx.latitude == null) { sgx.latitude = devLatitude }
    sgx.longitude = longitude
    if (sgx.longitude == null) { sgx.longitude = devLongitude }
    sgx.altitude = altitude
    if (sgx.altitude == null) { sgx.altitude = devAltitude }
    sgx.result = new ProcessorResult()
    sgx.dataReceived = dataReceived
    sgx.dataTimestamp = Number(receivedDataTimestamp)
    sgx.channelReader = channelReader
    sgx.status = status
    sgx.alert = alert
    sgx.virtualCommand = newCommand
    sgx.requestData = requestData
    sgx.deviceConfig = devConfig
    sgx.applicationConfig = appConfig
    sgx.timeOffsets = timeOffsets
    sgx.verify(dataReceived, status)
    //put original values. (todo: replace with verify)
    //if (dataReceived.length > 0) {
    //    for (i = 0; i < dataReceived.length; i++) {
    //        channelData = dataReceived[i];
    //        sgx.result.putData(channelData);
    //        sgx.result.log(channelData.toString());
    //    }
    //}
    sgx.result.setDeviceStatus(status);
    try {
        //injectedCode
    } catch (processorError) {
        sgx.addNotification('error', 'Device '+eui+' processor script error: '+processorError)
    }
    return sgx.result;
}

var processRawData = function (eui, requestBody, channelReader, userID, dataTimestamp) {
    var ChannelData = Java.type("com.signomix.common.iot.ChannelData");
    var IotEvent = Java.type("com.signomix.common.event.IotEvent");
    var ProcessorResult = Java.type("com.signomix.receiver.script.ProcessorResult");
    var channelData = {};

    var sgx = Object.create(sgx0)
    sgx.eui = eui
    sgx.result = new ProcessorResult()
    sgx.dataReceived = []
    sgx.dataTimestamp = dataTimestamp
    sgx.channelReader = channelReader
    //sgx.verify(dataReceived, state)
    try {
        //injectedCode
    } catch (processorError) {
        sgx.addNotification('error', 'Device '+eui+'processor script error: '+processorError)
    }
    return sgx.result;
}