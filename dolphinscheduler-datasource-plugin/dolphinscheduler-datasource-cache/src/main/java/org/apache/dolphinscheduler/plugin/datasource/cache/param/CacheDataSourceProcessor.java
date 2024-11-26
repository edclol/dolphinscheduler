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

import org.apache.dolphinscheduler.common.constants.Constants;
import org.apache.dolphinscheduler.common.constants.DataSourceConstants;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.plugin.datasource.api.datasource.AbstractDataSourceProcessor;
import org.apache.dolphinscheduler.plugin.datasource.api.datasource.BaseDataSourceParamDTO;
import org.apache.dolphinscheduler.plugin.datasource.api.datasource.DataSourceProcessor;
import org.apache.dolphinscheduler.plugin.datasource.api.utils.PasswordUtils;
import org.apache.dolphinscheduler.spi.datasource.BaseConnectionParam;
import org.apache.dolphinscheduler.spi.datasource.ConnectionParam;
import org.apache.dolphinscheduler.spi.enums.DbType;

import org.apache.commons.collections4.MapUtils;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auto.service.AutoService;

@AutoService(DataSourceProcessor.class)
public class CacheDataSourceProcessor extends AbstractDataSourceProcessor {

    private static final Logger log = LoggerFactory.getLogger(CacheDataSourceProcessor.class);

    @Override
    public BaseDataSourceParamDTO castDatasourceParamDTO(String paramJson) {
        return JSONUtils.parseObject(paramJson, CacheDataSourceParamDTO.class);
    }

    @Override
    public BaseDataSourceParamDTO createDatasourceParamDTO(String connectionJson) {
        CacheConnectionParam connectionParams = (CacheConnectionParam) createConnectionParams(connectionJson);
        CacheDataSourceParamDTO mysqlDatasourceParamDTO = new CacheDataSourceParamDTO();
        mysqlDatasourceParamDTO.setUserName(connectionParams.getUser());
        mysqlDatasourceParamDTO.setDatabase(connectionParams.getDatabase());
        String address = connectionParams.getAddress();
        try {
            String[] hostPortSplit = getHostPortSplit(address);
            mysqlDatasourceParamDTO.setPort(Integer.parseInt(hostPortSplit[1]));
            mysqlDatasourceParamDTO.setHost(hostPortSplit[0]);
        } catch (NumberFormatException e) {
            log.error("Invalid port number", e);
        }
        return mysqlDatasourceParamDTO;
    }

    public static String[] getHostPortSplit(String address) {
        String[] hostSeperator = address.split(Constants.DOUBLE_SLASH);
        if (hostSeperator.length < 2) {
            log.error("Invalid address format");
        }
        String[] hostPortArray = hostSeperator[hostSeperator.length - 1].split(Constants.COMMA);
        if (hostPortArray.length < 1) {
            log.error("Invalid hostPortArray format");
        }
        String[] hostPortSplit = hostPortArray[0].split(Constants.COLON);
        if (hostPortSplit.length < 2) {
            log.error("Invalid hostPortSplit format");
        }
        return hostPortSplit;
    }

    @Override
    public BaseConnectionParam createConnectionParams(BaseDataSourceParamDTO dataSourceParam) {
        CacheDataSourceParamDTO cacheDatasourceParam = (CacheDataSourceParamDTO) dataSourceParam;
        String address = String.format("%s%s:%s", DataSourceConstants.JDBC_CACHE, cacheDatasourceParam.getHost(),
                cacheDatasourceParam.getPort());
        String jdbcUrl = String.format("%s/%s", address, cacheDatasourceParam.getDatabase());

        CacheConnectionParam CacheConnectionParam = new CacheConnectionParam();
        CacheConnectionParam.setJdbcUrl(jdbcUrl);
        CacheConnectionParam.setValidationQuery(getValidationQuery());
        CacheConnectionParam.setDatabase(cacheDatasourceParam.getDatabase());
        CacheConnectionParam.setAddress(address);
        CacheConnectionParam.setUser(cacheDatasourceParam.getUserName());
        CacheConnectionParam.setPassword(PasswordUtils.encodePassword(cacheDatasourceParam.getPassword()));
        CacheConnectionParam.setDriverClassName(getDatasourceDriver());
        CacheConnectionParam.setOther(cacheDatasourceParam.getOther());

        return CacheConnectionParam;
    }

    @Override
    public ConnectionParam createConnectionParams(String connectionJson) {
        return JSONUtils.parseObject(connectionJson, CacheConnectionParam.class);
    }

    @Override
    public String getDatasourceDriver() {
        return DataSourceConstants.COM_INTERSYS_JDBC_CACHE_DRIVER;
    }

    @Override
    public String getValidationQuery() {
        return DataSourceConstants.CACHE_VALIDATION_QUERY;
    }

    @Override
    public String getJdbcUrl(ConnectionParam connectionParam) {
        CacheConnectionParam CacheConnectionParam = (CacheConnectionParam) connectionParam;
        String jdbcUrl = CacheConnectionParam.getJdbcUrl();
        Map<String, String> other = CacheConnectionParam.getOther();
        if (MapUtils.isNotEmpty(other)) {
            return String.format("%s?%s", jdbcUrl, transformOther(other));
        }
        return jdbcUrl;
    }

    String transformOther(Map<String, String> otherMap) {
        if (otherMap == null || otherMap.isEmpty()) {
            return "";
        }

        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry : otherMap.entrySet()) {
            try {
                String encodedKey = URLEncoder.encode(entry.getKey(), StandardCharsets.UTF_8.toString());
                String encodedValue = URLEncoder.encode(entry.getValue(), StandardCharsets.UTF_8.toString());
                sb.append(encodedKey).append("=").append(encodedValue).append("&");
            } catch (Exception e) {
                log.error("Error encoding key-value pair: {}={}", entry.getKey(), entry.getValue(), e);
            }
        }

        // 移除最后一个多余的 "&"
        if (sb.length() > 0) {
            sb.setLength(sb.length() - 1);
        }

        return sb.toString();
    }

    @Override
    public Connection getConnection(ConnectionParam connectionParam) throws ClassNotFoundException, SQLException {
        CacheConnectionParam mysqlConnectionParam = (CacheConnectionParam) connectionParam;
        Class.forName(getDatasourceDriver());
        String user = mysqlConnectionParam.getUser();
        String password = PasswordUtils.decodePassword(mysqlConnectionParam.getPassword());
        return DriverManager.getConnection(getJdbcUrl(connectionParam), user, password);
    }

    @Override
    public DbType getDbType() {
        return DbType.CACHE;
    }

    @Override
    public DataSourceProcessor create() {
        return new CacheDataSourceProcessor();
    }
}
