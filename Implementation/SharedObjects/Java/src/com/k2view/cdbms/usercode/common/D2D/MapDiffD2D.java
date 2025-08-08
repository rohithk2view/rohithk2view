package com.k2view.cdbms.usercode.common.D2D;

import java.util.*;
import java.sql.*;
import java.math.*;
import java.io.*;
import com.k2view.cdbms.shared.*;
import com.k2view.cdbms.sync.*;
import com.k2view.cdbms.lut.*;
import com.k2view.cdbms.shared.logging.LogEntry.*;


import com.k2view.broadway.model.Actor;
import com.k2view.broadway.model.Data;
import com.k2view.broadway.util.MapFactory;
import java.util.Map;
import java.util.Objects;

public class MapDiffD2D extends MapFactory implements Actor {
    public MapDiffD2D() {
    }

    public void action(Data input, Data output) {
        Map<?, ?> a = input.map("a");
        Map<?, ?> b = input.map("b");
        Map<Object, Object> m = this.createMap(input);
        m.putAll(b);
        b.forEach((k, v) -> {
            if (a.containsKey(k) ) {
                if (a.get(k) instanceof Number && v instanceof Number && Math.abs(((Number) a.get(k)).doubleValue() - ((Number) v).doubleValue()) == 0 ) {
                    m.remove(k);
                } else if (a.get(k) instanceof Number && v instanceof String && Objects.equals(a.get(k).toString(), v)) {
                    m.remove(k);
                } else if (a.get(k) instanceof String && v instanceof Number && Objects.equals(a.get(k), ((Number) v).toString())) {
                    m.remove(k);
                } else if (Objects.equals(a.get(k), v)) {
                        m.remove(k);
                }
            }
        });
        output.put("map", m);
    }
}
