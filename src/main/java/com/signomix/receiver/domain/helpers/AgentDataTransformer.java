package com.signomix.receiver.domain.helpers;

import com.signomix.common.iot.generic.IotData2;
import com.signomix.receiver.domain.dto.ReportDto;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@ApplicationScoped
public class AgentDataTransformer {

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
        return iotDataList;
    }
}
