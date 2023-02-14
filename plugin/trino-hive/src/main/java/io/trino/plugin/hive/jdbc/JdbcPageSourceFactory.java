/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.hive.jdbc;

import io.trino.plugin.hive.AcidInfo;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePageSourceFactory;
import io.trino.plugin.hive.ReaderColumns;
import io.trino.plugin.hive.ReaderPageSource;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.predicate.TupleDomain;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;

import static io.trino.plugin.hive.HivePageSourceProvider.projectBaseColumns;
import static java.util.stream.Collectors.toUnmodifiableList;

public class JdbcPageSourceFactory
        implements HivePageSourceFactory
{
    @Inject
    public JdbcPageSourceFactory()
    {}

    @Override
    public Optional<ReaderPageSource> createPageSource(
            Configuration configuration,
            ConnectorSession session,
            Path path,
            long start,
            long length,
            long estimatedFileSize,
            Properties schema,
            List<HiveColumnHandle> columns,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            Optional<AcidInfo> acidInfo,
            OptionalInt bucketNumber,
            boolean originalFile,
            AcidTransaction transaction)
    {
        if (schema.getProperty("classification") == null) {
            return Optional.empty();
        }

        if (!schema.getProperty("classification").equals("mysql")) {
            return Optional.empty();
        }

        Optional<ReaderColumns> readerProjections = projectBaseColumns(columns);
        List<HiveColumnHandle> baseColumns = readerProjections.map(projection ->
                        projection.get().stream()
                                .map(HiveColumnHandle.class::cast)
                                .collect(toUnmodifiableList()))
                .orElse(columns);

        ConnectorPageSource jdbcPageSource = new JdbcPageSource();

        return Optional.of(new ReaderPageSource(jdbcPageSource, readerProjections));
    }
}
