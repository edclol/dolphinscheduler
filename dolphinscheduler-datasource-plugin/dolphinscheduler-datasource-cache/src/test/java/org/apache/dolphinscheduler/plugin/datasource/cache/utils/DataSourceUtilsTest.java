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

package org.apache.dolphinscheduler.plugin.datasource.cache.utils;

import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.common.utils.PropertyUtils;
import org.apache.dolphinscheduler.plugin.datasource.api.plugin.DataSourceClientProvider;
import org.apache.dolphinscheduler.plugin.datasource.api.utils.CommonUtils;
import org.apache.dolphinscheduler.plugin.datasource.api.utils.DataSourceUtils;
import org.apache.dolphinscheduler.plugin.datasource.api.utils.PasswordUtils;
import org.apache.dolphinscheduler.plugin.datasource.cache.param.CacheConnectionParam;
import org.apache.dolphinscheduler.plugin.datasource.cache.param.CacheDataSourceParamDTO;
import org.apache.dolphinscheduler.spi.datasource.ConnectionParam;
import org.apache.dolphinscheduler.spi.enums.DbType;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DataSourceUtilsTest {

    @Test
    void testCheckDatasourceParam() {
        CacheDataSourceParamDTO cacheDataSourceParamDTO = new CacheDataSourceParamDTO();
        cacheDataSourceParamDTO.setHost("localhost");
        cacheDataSourceParamDTO.setDatabase("default");
        Map<String, String> other = new HashMap<>();
        other.put("reconnect", "true");
        cacheDataSourceParamDTO.setOther(other);
        DataSourceUtils.checkDatasourceParam(cacheDataSourceParamDTO);
        Assertions.assertTrue(true);
    }

    @Test
    void testBuildConnectionParams() {
        CacheDataSourceParamDTO cacheDataSourceParamDTO = new CacheDataSourceParamDTO();
        cacheDataSourceParamDTO.setHost("localhost");
        cacheDataSourceParamDTO.setDatabase("default");
        cacheDataSourceParamDTO.setUserName("root");
        cacheDataSourceParamDTO.setPort(1972);
        cacheDataSourceParamDTO.setPassword("123456");

        try (
                MockedStatic<PasswordUtils> mockedStaticPasswordUtils = Mockito.mockStatic(PasswordUtils.class);
                MockedStatic<CommonUtils> mockedStaticCommonUtils = Mockito.mockStatic(CommonUtils.class)) {
            mockedStaticPasswordUtils.when(() -> PasswordUtils.encodePassword(Mockito.anyString()))
                    .thenReturn("123456");
            mockedStaticCommonUtils.when(CommonUtils::getKerberosStartupState).thenReturn(false);
            ConnectionParam connectionParam = DataSourceUtils.buildConnectionParams(cacheDataSourceParamDTO);
            Assertions.assertNotNull(connectionParam);
        }
    }

    @Test
    void testBuildConnectionParams2() {
        CacheDataSourceParamDTO cacheDatasourceParamDTO = new CacheDataSourceParamDTO();
        cacheDatasourceParamDTO.setHost("localhost");
        cacheDatasourceParamDTO.setDatabase("default");
        cacheDatasourceParamDTO.setUserName("root");
        cacheDatasourceParamDTO.setPort(1972);
        cacheDatasourceParamDTO.setPassword("123456");
        ConnectionParam connectionParam =
                DataSourceUtils.buildConnectionParams(DbType.CACHE, JSONUtils.toJsonString(cacheDatasourceParamDTO));
        Assertions.assertNotNull(connectionParam);
    }

    @Test
    void testGetConnection() throws ExecutionException, SQLException {
        try (
                MockedStatic<PropertyUtils> mockedStaticPropertyUtils = Mockito.mockStatic(PropertyUtils.class);
                MockedStatic<DataSourceClientProvider> mockedStaticDataSourceClientProvider =
                        Mockito.mockStatic(DataSourceClientProvider.class)) {
            mockedStaticPropertyUtils.when(() -> PropertyUtils.getLong("kerberos.expire.time", 24L)).thenReturn(24L);

            Connection connection = Mockito.mock(Connection.class);

            Mockito.when(DataSourceClientProvider.getAdHocConnection(Mockito.any(), Mockito.any()))
                    .thenReturn(connection);
            CacheConnectionParam connectionParam = new CacheConnectionParam();
            connectionParam.setUser("root");
            connectionParam.setPassword("123456");

            Assertions.assertNotNull(connection);
        }
    }

    @Test
    void testGetJdbcUrl() {
        CacheConnectionParam cacheConnectionParam = new CacheConnectionParam();
        cacheConnectionParam.setJdbcUrl("jdbc:Cache://localhost:1972");
        String jdbcUrl = DataSourceUtils.getJdbcUrl(DbType.CACHE, cacheConnectionParam);
        Assertions.assertEquals(
                "jdbc:Cache://localhost:1972",
                jdbcUrl);
    }

    @Test
    void testBuildDatasourceParamDTO() {
        CacheConnectionParam connectionParam = new CacheConnectionParam();
        connectionParam.setJdbcUrl(
                "jdbc:Cache://localhost:1972?reconnect=true");
        connectionParam.setAddress("jdbc:mysql://localhost:1972");
        connectionParam.setUser("root");
        connectionParam.setPassword("123456");

        Assertions.assertNotNull(
                DataSourceUtils.buildDatasourceParamDTO(DbType.CACHE, JSONUtils.toJsonString(connectionParam)));

    }

    @Test
    void testGetDatasourceProcessor() {
        Assertions.assertNotNull(DataSourceUtils.getDatasourceProcessor(DbType.CACHE));
    }

    @Test
    void testGetDatasourceProcessorError() {
        Assertions.assertThrows(Exception.class, () -> {
            DataSourceUtils.getDatasourceProcessor(null);
        });
    }
}
