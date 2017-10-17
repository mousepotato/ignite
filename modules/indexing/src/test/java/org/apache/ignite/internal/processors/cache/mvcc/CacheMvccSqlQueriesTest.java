/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.mvcc;

import java.io.Serializable;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteInClosure;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
@SuppressWarnings("unchecked")
public class CacheMvccSqlQueriesTest extends CacheMvccAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testAccountsTxSql_SingleNode_SinglePartition() throws Exception {
        accountsTxReadAll(1, 0, 0, 1, new IgniteInClosure<CacheConfiguration>() {
            @Override public void apply(CacheConfiguration ccfg) {
                ccfg.setIndexedTypes(Integer.class, MvccTestAccount.class);
            }
        }, false, ReadMode.SQL_ALL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSqlSimple() throws Exception {
        Ignite srv0 = startGrid(0);

        IgniteCache<Integer, MvccTestSqlIndexValue> cache =  (IgniteCache)srv0.createCache(
            cacheConfiguration(PARTITIONED, FULL_SYNC, 0, DFLT_PARTITION_COUNT).
            setIndexedTypes(Integer.class, MvccTestSqlIndexValue.class));

        cache.put(1, new MvccTestSqlIndexValue(1));
        cache.put(1, new MvccTestSqlIndexValue(2));

        SqlQuery<Integer, MvccTestSqlIndexValue> qry =
            new SqlQuery<>(MvccTestSqlIndexValue.class, "_key >= 0");

        List<IgniteCache.Entry<Integer, MvccTestSqlIndexValue>> res = cache.query(qry).getAll();

        assertEquals(1, res.size());
    }

    /**
     *
     */
    static class MvccTestSqlIndexValue implements Serializable {
        /** */
        @QuerySqlField(index = true)
        private int idxVal1;

        /**
         * @param idxVal1 Indexed value 1.
         */
        public MvccTestSqlIndexValue(int idxVal1) {
            this.idxVal1 = idxVal1;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MvccTestSqlIndexValue.class, this);
        }
    }
}
