/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.plugin.datasource.cache.param;

import org.apache.dolphinscheduler.common.constants.DataSourceConstants;
import org.apache.dolphinscheduler.plugin.datasource.api.utils.PasswordUtils;
import org.apache.dolphinscheduler.spi.enums.DbType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CacheDataSourceProcessorTest {

    @InjectMocks
    private CacheDataSourceProcessor cacheDataSourceProcessor;

    @Test
    public void testTransformOtherWithValidMap() {
        Map<String, String> otherMap = new HashMap<>();
        otherMap.put("key1", "value1");
        otherMap.put("key2", "value2");

        String result = cacheDataSourceProcessor.transformOther(otherMap);

        Assertions.assertEquals("key1=value1&key2=value2", result);
    }

    @Test
    public void testTransformOtherWithEmptyMap() {
        Map<String, String> otherMap = new HashMap<>();
        String result = cacheDataSourceProcessor.transformOther(otherMap);
        Assertions.assertEquals("", result);
    }

    @Test
    void testCreateConnectionParams() {
        Map<String, String> props = new HashMap<>();
        props.put("service principal", "null");
        props.put("TransactionIsolationLevel", "TRANSACTION_READ_UNCOMMITTED");
        CacheDataSourceParamDTO mysqlDatasourceParamDTO = new CacheDataSourceParamDTO();
        mysqlDatasourceParamDTO.setUserName("root");
        mysqlDatasourceParamDTO.setPassword("123456");
        mysqlDatasourceParamDTO.setHost("localhost");
        mysqlDatasourceParamDTO.setPort(1972);
        mysqlDatasourceParamDTO.setDatabase("default");
        mysqlDatasourceParamDTO.setOther(props);

        try (MockedStatic<PasswordUtils> ignored = Mockito.mockStatic(PasswordUtils.class)) {
            Mockito.when(PasswordUtils.encodePassword(Mockito.anyString())).thenReturn("123456");
            CacheConnectionParam connectionParams = (CacheConnectionParam) cacheDataSourceProcessor
                    .createConnectionParams(mysqlDatasourceParamDTO);
            Assertions.assertEquals("jdbc:Cache://localhost:1972", connectionParams.getAddress());
            Assertions.assertEquals("123456", connectionParams.getPassword());
            Assertions.assertEquals("jdbc:Cache://localhost:1972/default",
                    connectionParams.getJdbcUrl());
            String jdbcUrl = cacheDataSourceProcessor.getJdbcUrl(connectionParams);
            Assertions.assertEquals(
                    "jdbc:Cache://localhost:1972/default?service+principal=null&TransactionIsolationLevel=TRANSACTION_READ_UNCOMMITTED",
                    jdbcUrl);

        }
    }

    @Test
    void testCreateConnectionParams2() {
        String connectionJson =
                "{\"user\":\"root\",\"password\":\"123456\",\"address\":\"jdbc:Cache://localhost:1972\""
                        + ",\"database\":\"default\",\"jdbcUrl\":\"jdbc:Cache://localhost:1972/default\"}";
        CacheConnectionParam connectionParams = (CacheConnectionParam) cacheDataSourceProcessor
                .createConnectionParams(connectionJson);
        Assertions.assertNotNull(connectionJson);
        Assertions.assertEquals("root", connectionParams.getUser());
    }

    @Test
    void testGetDatasourceDriver() {
        Assertions.assertEquals(DataSourceConstants.COM_INTERSYS_JDBC_CACHE_DRIVER,
                cacheDataSourceProcessor.getDatasourceDriver());
    }

    @Test
    void testGetDbType() {
        Assertions.assertEquals(DbType.CACHE, cacheDataSourceProcessor.getDbType());
    }

    @Test
    void testGetValidationQuery() {
        Assertions.assertEquals(DataSourceConstants.CACHE_VALIDATION_QUERY,
                cacheDataSourceProcessor.getValidationQuery());
    }

    @Test
    void testGetDatasourceUniqueId() {
        CacheConnectionParam mysqlConnectionParam = new CacheConnectionParam();
        mysqlConnectionParam.setJdbcUrl("jdbc:Cache://localhost:1972/default");
        mysqlConnectionParam.setUser("root");
        mysqlConnectionParam.setPassword("123456");
        try (MockedStatic<PasswordUtils> mockedPasswordUtils = Mockito.mockStatic(PasswordUtils.class)) {
            Mockito.when(PasswordUtils.encodePassword(Mockito.anyString())).thenReturn("123456");
            Assertions.assertEquals("cache@root@123456@jdbc:Cache://localhost:1972/default",
                    cacheDataSourceProcessor.getDatasourceUniqueId(mysqlConnectionParam, DbType.CACHE));
        }
    }

    @Test
    void testGetHostPortSplit() {
        String address = "jdbc:Cache://localhost:1972";
        String[] expected = {"localhost", "1972"};
        String[] result = CacheDataSourceProcessor.getHostPortSplit(address);
        Assertions.assertArrayEquals(expected, result);
    }

    @Test
    void testGetHostPortSplit_InvalidFormat() {
        String invalidAddress = "invalid-format";
        String[] hostPortSplit = CacheDataSourceProcessor.getHostPortSplit(invalidAddress);
        Assertions.assertEquals("[invalid-format]", Arrays.toString(hostPortSplit));

    }

}
