package com.signomix.common.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.signomix.common.event.IotEvent;
import com.signomix.common.iot.Alert;
import com.signomix.common.iot.ChannelData;
import com.signomix.common.iot.Device;
import com.signomix.common.iot.virtual.VirtualData;

import org.jboss.logging.Logger;

import io.agroal.api.AgroalDataSource;

public class IotDatabaseDao implements IotDatabaseIface {
    private static final Logger LOG = Logger.getLogger(IotDatabaseDao.class);

    private AgroalDataSource dataSource;

    // TODO: get requestLimit from config
    private long requestLimit = 500;

    @Override
    public void setDatasource(AgroalDataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void updateDeviceStatus(String eui, Double newStatus, long timestamp, long lastFrame, String downlink,
            String deviceId) throws IotDatabaseException {
        Device device = getDevice(eui);
        if (device == null) {
            throw new IotDatabaseException(IotDatabaseException.NOT_FOUND, "device not found", null);
        }
        device.setState(newStatus);
        Device previous = getDevice(device.getEUI());
        String query;
        if (null != newStatus) {
            query = "update devices set lastseen=?,lastframe=?,downlink=?,devid=?,state=? where eui=?";
        } else {
            query = "update devices set lastseen=?,lastframe=?,downlink=?,devid=? where eui=?";
        }
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setLong(1, timestamp);
            pstmt.setLong(2, lastFrame);
            pstmt.setString(3, downlink);
            pstmt.setString(4, deviceId);
            if (null != newStatus) {
                pstmt.setDouble(5, newStatus);
                pstmt.setString(6, eui);
            } else {
                pstmt.setString(5, eui);
            }
            int updated = pstmt.executeUpdate();
            if (updated < 1) {
                throw new IotDatabaseException(IotDatabaseException.UNKNOWN,
                        "DB error updating device " + device.getEUI(), null);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage(), null);
        }
    }

    @Override
    public void putVirtualData(Device device, VirtualData data) throws IotDatabaseException {
        JsonMapper mapper = new JsonMapper();
        String serialized;
        try {
            serialized = mapper.writeValueAsString(data);
            LOG.debug(serialized);
        } catch (JsonProcessingException e) {
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, "", null);
        }
        String query = "MERGE INTO virtualdevicedata (eui, tstamp, data) KEY (eui) values (?,?,?)";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setString(1, device.getEUI());
            pst.setTimestamp(2, new Timestamp(data.timestamp));
            pst.setString(3, serialized);
            pst.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }
    }

    @Override
    public void putData(Device device, ArrayList<ChannelData> values) throws IotDatabaseException {
        if (values == null || values.isEmpty()) {
            System.out.println("no values");
            return;
        }
        int limit = 24;
        List channelNames = getDeviceChannels(device.getEUI());
        String query = "insert into devicedata (eui,userid,day,dtime,tstamp,d1,d2,d3,d4,d5,d6,d7,d8,d9,d10,d11,d12,d13,d14,d15,d16,d17,d18,d19,d20,d21,d22,d23,d24,project,state) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        long timestamp = values.get(0).getTimestamp();
        java.sql.Date date = new java.sql.Date(timestamp);
        java.sql.Time time = new java.sql.Time(timestamp);
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setString(1, device.getEUI());
            pst.setString(2, device.getUserID());
            pst.setDate(3, date);
            pst.setTime(4, time);
            pst.setTimestamp(5, new java.sql.Timestamp(timestamp));
            for (int i = 1; i <= limit; i++) {
                pst.setNull(i + 5, java.sql.Types.DOUBLE);
            }
            int index = -1;
            // if (values.size() <= limit) {
            // limit = values.size();
            // }
            if (values.size() > limit) {
                // TODO: send notification to the user?
            }
            for (int i = 1; i <= limit; i++) {
                if (i <= values.size()) {
                    index = channelNames.indexOf(values.get(i - 1).getName());
                    if (index >= 0 && index < limit) { // TODO: there must be control of mthe number of measures while
                        // defining device, not here
                        try {
                            pst.setDouble(6 + index, values.get(i - 1).getValue());
                        } catch (NullPointerException e) {
                            pst.setNull(6 + index, Types.DOUBLE);
                        }
                    }
                }
            }
            pst.setString(30, device.getProject());
            pst.setDouble(31, device.getState());
            pst.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }

    }

    @Override
    public Device getDevice(String userID, String deviceEUI, boolean withShared) throws IotDatabaseException {
        String query;
        if (withShared) {
            query = buildDeviceQuery()
                    + " AND (upper(d.eui)=upper(?) AND (d.userid = ? OR d.team like ? OR d.administrators like ?))";
        } else {
            query = buildDeviceQuery() + " AND ( upper(d.eui)=upper(?) and d.userid = ?)";
        }
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, deviceEUI);
            pstmt.setString(2, userID);
            if (withShared) {
                pstmt.setString(3, "%," + userID + ",%");
                pstmt.setString(4, "%," + userID + ",%");
            }
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                Device device = buildDevice(rs);
                return device;
            } else {
                return null;
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }
    }

    @Override
    public Device getDevice(String deviceEUI) throws IotDatabaseException {
        String query = buildDeviceQuery() + " AND ( upper(d.eui) = upper(?))";
        if (deviceEUI == null || deviceEUI.isEmpty()) {
            return null;
        }
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, deviceEUI);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                Device device = buildDevice(rs);
                return device;
            } else {
                return null;
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }
    }

    @Override
    public List<String> getDeviceChannels(String deviceEUI) throws IotDatabaseException {
        List<String> channels;
        String query = "select channels from devicechannels where eui=?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setString(1, deviceEUI);
            ResultSet rs = pst.executeQuery();
            if (rs.next()) {
                String[] s = rs.getString(1).toLowerCase().split(",");
                channels = Arrays.asList(s);
                String channelStr = "";
                for (int i = 0; i < channels.size(); i++) {
                    channelStr = channelStr + channels.get(i) + ",";
                }
                LOG.debug("CHANNELS READ: " + deviceEUI + " " + channelStr);
                return channels;
            } else {
                return new ArrayList<>();
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }
    }

    @Override
    public List<List> getValues(String userID, String deviceID, String dataQuery)
            throws IotDatabaseException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ChannelData getLastValue(String userID, String deviceEUI, String channel) throws IotDatabaseException {
        int channelIndex = getChannelIndex(deviceEUI, channel);
        if (channelIndex < 0) {
            return null;
        }
        String columnName = "d" + (channelIndex);
        String query = "select eui,userid,day,dtime,tstamp," + columnName
                + " from devicedata where eui=? order by tstamp desc limit 1";
        ChannelData result = null;
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setString(1, deviceEUI);
            ResultSet rs = pst.executeQuery();
            Double d;
            if (rs.next()) {
                d = rs.getDouble(6);
                if (!rs.wasNull()) {
                    result = new ChannelData(deviceEUI, channel, d, rs.getTimestamp(5).getTime());
                }
            }
            return result;
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }
    }

    @Override
    public List<List> getLastValues(String userID, String deviceEUI) throws IotDatabaseException {
        String query = "select eui,userid,day,dtime,tstamp,d1,d2,d3,d4,d5,d6,d7,d8,d9,d10,d11,d12,d13,d14,d15,d16,d17,d18,d19,d20,d21,d22,d23,d24 from devicedata where eui=? order by tstamp desc limit 1";
        List<String> channels = getDeviceChannels(deviceEUI);
        ArrayList<ChannelData> row = new ArrayList<>();
        ArrayList<List> result = new ArrayList<>();
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setString(1, deviceEUI);
            ResultSet rs = pst.executeQuery();
            double d;
            if (rs.next()) {
                for (int i = 0; i < channels.size(); i++) {
                    d = rs.getDouble(6 + i);
                    if (!rs.wasNull()) {
                        row.add(new ChannelData(deviceEUI, channels.get(i), d,
                                rs.getTimestamp(5).getTime()));
                    }
                }
                result.add(row);
            }
            return result;
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }
    }

    private Device buildDevice(ResultSet rs) throws SQLException {
        // select
        // eui,name,userid,type,team,channels,code,decoder,key,description,
        // lastseen,tinterval,lastframe,template,pattern,downlink,commandscript,appid,groups,alert,
        // appeui,devid,active,project,latitude,longitude,altitude,state,retention,administrators,
        // framecheck,configuration,organization,organizationapp,a.config from devices
        // as d left join applications as a
        Device d = new Device();
        d.setEUI(rs.getString(1));
        d.setName(rs.getString(2));
        d.setUserID(rs.getString(3));
        d.setType(rs.getString(4));
        d.setTeam(rs.getString(5));
        d.setChannels(rs.getString(6));
        d.setCode(rs.getString(7));
        d.setEncoder(rs.getString(8));
        d.setKey(rs.getString(9));
        d.setDescription(rs.getString(10));
        d.setLastSeen(rs.getLong(11));
        d.setTransmissionInterval(rs.getLong(12));
        d.setLastFrame(rs.getLong(13));
        d.setTemplate(rs.getString(14));
        d.setPattern(rs.getString(15));
        d.setDownlink(rs.getString(16));
        d.setCommandScript(rs.getString(17));
        d.setApplicationID(rs.getString(18));
        d.setGroups(rs.getString(19));
        d.setAlertStatus(rs.getInt(20));
        d.setApplicationEUI(rs.getString(21));
        d.setDeviceID(rs.getString(22));
        d.setActive(rs.getBoolean(23));
        d.setProject(rs.getString(24));
        d.setLatitude(rs.getDouble(25));
        d.setLongitude(rs.getDouble(26));
        d.setAltitude(rs.getDouble(27));
        d.setState(rs.getDouble(28));
        d.setRetentionTime(rs.getLong(29));
        d.setAdministrators(rs.getString(30));
        d.setCheckFrames(rs.getBoolean(31));
        d.setConfiguration(rs.getString(32));
        d.setOrganizationId(rs.getLong(33));
        d.setOrgApplicationId(rs.getLong(34));
        d.setApplicationConfig(rs.getString(35));
        return d;
    }

    @Override
    public IotEvent getFirstCommand(String deviceEUI) throws IotDatabaseException {
        String query = "select id,category,type,origin,payload,createdat from commands where origin like ? order by createdat limit 1";
        IotEvent result = null;
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setString(1, "%@" + deviceEUI);
            //pst.setString(1, deviceEUI);
            ResultSet rs = pst.executeQuery();
            if (rs.next()) {
                // result = new IotEvent(deviceEUI, rs.getString(2), rs.getString(3), null,
                // rs.getString(4));
                result = new IotEvent();
                result.setId(rs.getLong(1));
                result.setCategory(rs.getString(2));
                result.setType(rs.getString(3));
                result.setOrigin(rs.getString(4));
                result.setPayload(rs.getString(5));
                result.setCreatedAt(rs.getLong(6));
            }
            return result;
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }
    }
    @Override
    public long getMaxCommandId() throws IotDatabaseException {
        String query = "SELECT  max(commands.id), max(commandslog.id) FROM commands CROSS JOIN commandslog";
        long result = 0;
        long v1=0;
        long v2=0;
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            ResultSet rs = pst.executeQuery();
            if (rs.next()) {
                v1=rs.getLong(1);
                v1=rs.getLong(2);
            }
            if(v1>v2){
                result=v1;
            }else{
                result=v2;
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION);
        }
        return result;
    }
    @Override
    public long getMaxCommandId(String deviceEui) throws IotDatabaseException {
        String query = "SELECT  max(commands.id), max(commandslog.id) FROM commands CROSS JOIN commandslog "
        +"WHERE commands.origin=commandslog.origin AND commands.origin like %@"+deviceEui;
        long result = 0;
        long v1=0;
        long v2=0;
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            ResultSet rs = pst.executeQuery();
            if (rs.next()) {
                v1=rs.getLong(1);
                v1=rs.getLong(2);
            }
            if(v1>v2){
                result=v1;
            }else{
                result=v2;
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION);
        }
        return result;
    }


    @Override
    public void removeCommand(long id) throws IotDatabaseException {
        String query = "delete from commands where id=?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setLong(1, id);
            pst.executeUpdate();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }
    }

    @Override
    public void putCommandLog(String deviceEUI, IotEvent commandEvent) throws IotDatabaseException {
        String query = "insert into commandslog (id,category,type,origin,payload,createdat) values (?,?,?,?,?,?);";
        String command = (String) commandEvent.getPayload();
        if (command.startsWith("#") || command.startsWith("&")) {
            command = command.substring(1);
        }
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setLong(1, commandEvent.getId());
            pst.setString(2, commandEvent.getCategory());
            pst.setString(3, commandEvent.getType());
            pst.setString(4, deviceEUI);
            pst.setString(5, command);
            pst.setLong(6, commandEvent.getCreatedAt());
            pst.executeUpdate();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }
    }

    @Override
    public void putDeviceCommand(String deviceEUI, IotEvent commandEvent) throws IotDatabaseException {
        String query = "insert into commands (id,category,type,origin,payload,createdat) values (?,?,?,?,?,?);";
        String query2 = "merge into commands (id,category,type,origin,payload,createdat) key (id) values (?,?,?,?,?,?)";
        String command = (String) commandEvent.getPayload();
        boolean overwriteMode = false;
        if (command.startsWith("&")) {
            overwriteMode = false;
        } else if (command.startsWith("#")) {
            query = query2;
            overwriteMode = true;
        }
        command = command.substring(1);
        String origin=commandEvent.getOrigin();
        if(null==origin||origin.isEmpty()){
            origin=deviceEUI;
        }
        try (Connection conn = dataSource.getConnection(); PreparedStatement pst = conn.prepareStatement(query);) {
            pst.setLong(1, commandEvent.getId());
            pst.setString(2, commandEvent.getCategory());
            pst.setString(3, commandEvent.getType());
            pst.setString(4, origin);
            pst.setString(5, command);
            pst.setLong(6, commandEvent.getCreatedAt());
            pst.executeUpdate();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }

    }

    @Override
    public void addAlert(IotEvent event) throws IotDatabaseException {
        Alert alert = new Alert(event);
        String query = "insert into alerts (name,category,type,deviceeui,userid,payload,timepoint,serviceid,uuid,calculatedtimepoint,createdat,rooteventid,cyclic) values (?,?,?,?,?,?,?,?,?,?,?,?,?)";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, alert.getName());
            pstmt.setString(2, alert.getCategory());
            pstmt.setString(3, alert.getType());
            pstmt.setString(4, alert.getDeviceEUI());
            pstmt.setString(5, alert.getUserID());
            pstmt.setString(6, (null != alert.getPayload()) ? alert.getPayload().toString() : "");
            pstmt.setString(7, "");
            pstmt.setString(8, "");
            pstmt.setString(9, "");
            pstmt.setLong(10, 0);
            pstmt.setLong(11, alert.getCreatedAt());
            pstmt.setLong(12, -1);
            pstmt.setBoolean(13, false);
            int updated = pstmt.executeUpdate();
            if (updated < 1) {
                throw new IotDatabaseException(IotDatabaseException.UNKNOWN,
                        "Unable to create notification " + alert.getId(), null);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            e.printStackTrace();
            throw new IotDatabaseException(IotDatabaseException.UNKNOWN, e.getMessage(), null);
        }
    }

    @Override
    public List getAlerts(String userID, boolean descending) throws IotDatabaseException {
        String query = "select id,name,category,type,deviceeui,userid,payload,timepoint,serviceid,uuid,calculatedtimepoint,createdat,rooteventid,cyclic from alerts where userid = ? order by id ";
        if (descending) {
            query = query.concat(" desc");
        }
        query = query.concat(" limit ?");
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, userID);
            pstmt.setLong(2, requestLimit);
            ResultSet rs = pstmt.executeQuery();
            ArrayList<Alert> list = new ArrayList<>();
            while (rs.next()) {
                list.add(buildAlert(rs));
            }
            return list;
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }
    }

    @Override
    public void removeAlert(long alertID) throws IotDatabaseException {
        String query = "delete from alerts where id=?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setLong(1, alertID);
            int updated = pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void removeAlerts(String userID) throws IotDatabaseException {
        String query = "delete from alerts where userid=?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, userID);
            int updated = pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void removeAlerts(String userID, long checkpoint) throws IotDatabaseException {
        String query = "delete from alerts where userid=? and createdat < ?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, userID);
            pstmt.setLong(2, checkpoint);
            int updated = pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void removeOutdatedAlerts(long checkpoint) throws IotDatabaseException {
        String query = "delete from alerts where createdat < ?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setLong(1, checkpoint);
            int updated = pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public int getChannelIndex(String deviceEUI, String channel) throws IotDatabaseException {
        return getDeviceChannels(deviceEUI).indexOf(channel) + 1;
    }

    Alert buildAlert(ResultSet rs) throws SQLException {
        // id,name,category,type,deviceeui,userid,payload,timepoint,serviceid,uuid,calculatedtimepoint,createdat,rooteventid,cyclic
        // Device d = new Device();
        Alert a = new Alert();
        a.setId(rs.getLong(1));
        a.setName(rs.getString(2));
        a.setCategory(rs.getString(3));
        a.setType(rs.getString(4));
        a.setOrigin(rs.getString(6) + "\t" + rs.getString(5));
        a.setPayload(rs.getString(7));
        a.setCreatedAt(rs.getLong(12));
        return a;
    }

    private String buildDeviceQuery() {
        String query = "SELECT"
                + " d.eui, d.name, d.userid, d.type, d.team, d.channels, d.code, d.decoder, d.key, d.description, d.lastseen, d.tinterval,"
                + " d.lastframe, d.template, d.pattern, d.downlink, d.commandscript, d.appid, d.groups, d.alert,"
                + " d.appeui, d.devid, d.active, d.project, d.latitude, d.longitude, d.altitude, d.state, d.retention,"
                + " d.administrators, d.framecheck, d.configuration, d.organization, d.organizationapp, a.configuration FROM devices AS d"
                + " LEFT JOIN applications AS a WHERE d.organizationapp=a.id";
        return query;
    }

}
