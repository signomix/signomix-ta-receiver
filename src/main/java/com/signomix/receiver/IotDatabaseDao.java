package com.signomix.receiver;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.signomix.receiver.dto.ChannelData;
import com.signomix.receiver.dto.Device;

import org.jboss.logging.Logger;

import io.agroal.api.AgroalDataSource;

public class IotDatabaseDao implements IotDatabaseIface {
    private static final Logger LOG = Logger.getLogger(IotDatabaseDao.class);

    private AgroalDataSource dataSource;

    @Override
    public void setDatasource(AgroalDataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void updateDeviceStatus(String eui, Double newStatus) throws IotDatabaseException {
        Device device = getDevice(eui);
        if (device == null) {
            throw new IotDatabaseException(IotDatabaseException.NOT_FOUND, "device not found", null);
        }
        device.setState(newStatus);
        Device previous = getDevice(device.getEUI());
        String query = "update devices set name=?,userid=?,type=?,team=?,channels=?,code=?,decoder=?,key=?,description=?,lastseen=?,tinterval=?,lastframe=?,template=?,pattern=?,downlink=?,commandscript=?,appid=?,appeui=?,groups=?,alert=?,devid=?,active=?,project=?,latitude=?,longitude=?,altitude=?,state=?, retention=?, administrators=?, framecheck=? where eui=?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement pstmt = conn.prepareStatement(query);) {
            pstmt.setString(1, device.getName());
            pstmt.setString(2, device.getUserID());
            pstmt.setString(3, device.getType());
            pstmt.setString(4, device.getTeam());
            pstmt.setString(5, device.getChannelsAsString());
            pstmt.setString(6, device.getCode());
            pstmt.setString(7, device.getEncoder());
            pstmt.setString(8, device.getKey());
            pstmt.setString(9, device.getDescription());
            pstmt.setLong(10, device.getLastSeen());
            pstmt.setLong(11, device.getTransmissionInterval());
            pstmt.setLong(12, device.getLastFrame());
            pstmt.setString(13, device.getTemplate());
            pstmt.setString(14, device.getPattern());
            pstmt.setString(15, device.getDownlink());
            pstmt.setString(16, device.getCommandScript());
            pstmt.setString(17, device.getApplicationID());
            pstmt.setString(18, device.getApplicationEUI());
            pstmt.setString(19, device.getGroups());
            pstmt.setInt(20, device.getAlertStatus());
            pstmt.setString(21, device.getDeviceID());
            pstmt.setBoolean(22, device.isActive());
            pstmt.setString(23, device.getProject());
            pstmt.setDouble(24, device.getLatitude());
            pstmt.setDouble(25, device.getLongitude());
            pstmt.setDouble(26, device.getAltitude());
            pstmt.setDouble(27, device.getState());
            pstmt.setLong(28, device.getRetentionTime());
            pstmt.setString(29, device.getAdministrators());
            pstmt.setBoolean(30, device.isCheckFrames());
            pstmt.setString(31, device.getEUI());
            // TODO: last frame
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
            query = "select eui,name,userid,type,team,channels,code,decoder,key,description,lastseen,tinterval,lastframe,template,pattern,downlink,commandscript,appid,appeui,groups,alert,devid,active,project,latitude,longitude,altitude,state,retention,administrators,framecheck from devices where upper(eui)=upper(?) and (userid = ? or team like ? or administrators like ?)";
        } else {
            query = "select eui,name,userid,type,team,channels,code,decoder,key,description,lastseen,tinterval,lastframe,template,pattern,downlink,commandscript,appid,appeui,groups,alert,devid,active,project,latitude,longitude,altitude,state,retention,administrators,framecheck from devices where upper(eui)=upper(?) and userid = ?";
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
        String query = "select eui,name,userid,type,team,channels,code,decoder,key,description,lastseen,tinterval,lastframe,template,pattern,downlink,commandscript,appid,appeui,groups,alert,devid,active,project,latitude,longitude,altitude,state,retention,administrators,framecheck from devices where upper(eui) = upper(?)";
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
                LOG.info("CHANNELS READ: " + deviceEUI + " " + channelStr);
                return channels;
            } else {
                return new ArrayList<>();
            }
        } catch (SQLException e) {
            throw new IotDatabaseException(IotDatabaseException.SQL_EXCEPTION, e.getMessage(), e);
        }
    }

    @Override
    public ChannelData getLastValue(String userID, String deviceID, String channel) throws IotDatabaseException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<List> getValues(String userID, String deviceID, String dataQuery) throws IotDatabaseException {
        // TODO Auto-generated method stub
        return null;
    }

    private Device buildDevice(ResultSet rs) throws SQLException {
        // eui,name,userid,type,team,channels,code,decoder,key,description,lastseen,tinterval
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
        d.setApplicationEUI(rs.getString(19));
        d.setGroups(rs.getString(20));
        d.setAlertStatus(rs.getInt(21));
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
        return d;
    }

}
