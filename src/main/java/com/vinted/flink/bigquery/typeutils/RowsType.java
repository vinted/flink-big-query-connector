package com.vinted.flink.bigquery.typeutils;

import com.google.cloud.bigquery.TableId;
import com.vinted.flink.bigquery.model.Rows;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import java.util.Map;

public class RowsType {
    public static <A> TypeInformation<Rows<A>> of(TypeInformation<A> typeInfo) {
        return Types.POJO((Class<Rows<A>>)(Class<?>)Rows.class, Map.of(
                "data", Types.LIST(typeInfo),
                "offset", Types.LONG,
                "stream", Types.STRING,
                "table", TypeInformation.of(TableId.class)
        ));
    }
}
