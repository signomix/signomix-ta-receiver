package com.signomix.receiver.domain.helpers;

import com.signomix.common.iot.ChannelData;
import com.signomix.common.iot.generic.IotData2;
import com.signomix.receiver.domain.dto.DockerContainerDto;
import com.signomix.receiver.domain.dto.ReportDto;
import com.signomix.receiver.domain.dto.ServerDto;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.jboss.logging.Logger;

@ApplicationScoped
public class AgentDataTransformer {

    private static final String SERVER_EUI_KEY = "serverEui";
    private static final String SERVER_MEMORY_TOTAL_CHANNEL = "memoryTotal";
    private static final String SERVER_MEMORY_FREE_CHANNEL = "memoryFree";
    private static final String CPU_USAGE_CHANNEL = "cpuUsage";
    private static final String MEMORY_USAGE_CHANNEL = "memoryUsage";
    private static final String MEMORY_LIMIT_CHANNEL = "memoryLimit";
    private static final String NET_IN_CHANNEL = "netIn";
    private static final String NET_OUT_CHANNEL = "netOut";
    private static final String BLOCK_IN_CHANNEL = "blockIn";
    private static final String BLOCK_OUT_CHANNEL = "blockOut";

    @Inject
    Logger logger;

    /**
     * Transforms the given ReportDto data into a list of IotData2 objects based on the provided configuration.
     * @param data
     * @param configuration
     * @return
     */
    public List<IotData2> transform(
        ReportDto data,
        HashMap<String, Object> configuration
    ) {
        ArrayList<IotData2> iotDataList = new ArrayList<>();
        // TODO: implement transformation logic based on the ReportDto and configuration
        // IotData2.normalize() is required?
        long timestamp = data.getTimestamp();
        // Add server IotData2 objects to the list
        ServerDto serverDto = data.getServer();
        IotData2 serverData = new IotData2();
        serverData.timestampUTC = Timestamp.from(
            Instant.ofEpochMilli(timestamp)
        );
        serverData.dev_eui = configuration.get(SERVER_EUI_KEY).toString();
        if (serverData.dev_eui != null) {
            serverData.dataList.add(
                new ChannelData(
                    SERVER_MEMORY_TOTAL_CHANNEL,
                    serverDto.memoryTotal(),
                    timestamp
                )
            );
            serverData.dataList.add(
                new ChannelData(
                    SERVER_MEMORY_FREE_CHANNEL,
                    serverDto.memoryFree(),
                    timestamp
                )
            );
            iotDataList.add(serverData);
        } else {
            logger.warn("No dev_eui found for server " + serverDto.name());
        }
        // Add containers IotData2 objects to the list
        for (DockerContainerDto containerDto : data.getContainers()) {
            IotData2 containerData = new IotData2();
            containerData.timestampUTC = serverData.timestampUTC;
            containerData.dev_eui = configuration
                .get(containerDto.name())
                .toString();
            if (containerData.dev_eui == null) {
                logger.warn(
                    "No dev_eui found for container: " + containerDto.name()
                );
                continue;
            }
            containerData.dataList.add(
                new ChannelData(
                    CPU_USAGE_CHANNEL,
                    containerDto.cpuUsage(),
                    timestamp
                )
            );
            containerData.dataList.add(
                new ChannelData(
                    MEMORY_USAGE_CHANNEL,
                    containerDto.memoryUsage(),
                    timestamp
                )
            );
            containerData.dataList.add(
                new ChannelData(
                    MEMORY_LIMIT_CHANNEL,
                    containerDto.memoryLimit(),
                    timestamp
                )
            );
            containerData.dataList.add(
                new ChannelData(NET_IN_CHANNEL, containerDto.netIn(), timestamp)
            );
            containerData.dataList.add(
                new ChannelData(
                    NET_OUT_CHANNEL,
                    containerDto.netOut(),
                    timestamp
                )
            );
            containerData.dataList.add(
                new ChannelData(
                    BLOCK_IN_CHANNEL,
                    containerDto.blockIn(),
                    timestamp
                )
            );
            containerData.dataList.add(
                new ChannelData(
                    BLOCK_OUT_CHANNEL,
                    containerDto.memoryUsage(),
                    timestamp
                )
            );
            iotDataList.add(containerData);
        }
        return iotDataList;
    }
}
