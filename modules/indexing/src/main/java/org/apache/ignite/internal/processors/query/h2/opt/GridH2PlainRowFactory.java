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

package org.apache.ignite.internal.processors.query.h2.opt;

import org.apache.ignite.internal.processors.cache.mvcc.MvccCoordinatorVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.h2.result.Row;
import org.h2.result.RowFactory;
import org.h2.value.Value;

/**
 * Row factory.
 */
public class GridH2PlainRowFactory extends RowFactory {
    /**
     * @param v Value.
     * @return Row.
     */
    public static Row create(Value v) {
        return new RowKey(v);
    }

    /**
     * TODO IGNITE-3478: review usages.
     *
     * @param ctx Query context.
     * @param data Values.
     * @return Row.
     */
    public static Row create(GridH2QueryContext ctx, Value... data) {
        MvccCoordinatorVersion mvccVer = ctx != null ? ctx.mvccVersion() : null;

        switch (data.length) {
            case 0:
                throw new IllegalStateException("Zero columns row.");

            case 1:
                return mvccVer != null ? new RowKeyMvcc(data[0], mvccVer) : new RowKey(data[0]);

            case 2:
                return mvccVer != null ? new RowPairMvcc(data[0], data[1], mvccVer) : new RowPair(data[0], data[1]);

            default:
                return mvccVer != null ? new RowSimpleMvcc(data, mvccVer) : new RowSimple(data);
        }
    }

    /** {@inheritDoc} */
    @Override public Row createRow(Value[] data, int memory) {
        GridH2QueryContext ctx = GridH2QueryContext.get();

        return create(ctx, data);
    }

    /**
     * Single value row.
     */
    private static class RowKey extends GridH2SearchRowAdapter {
        /** */
        private Value key;

        /**
         * @param key Key.
         */
        RowKey(Value key) {
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public int getColumnCount() {
            return 1;
        }

        /** {@inheritDoc} */
        @Override public Value getValue(int idx) {
            assert idx == 0 : idx;
            return key;
        }

        /** {@inheritDoc} */
        @Override public void setValue(int idx, Value v) {
            assert idx == 0 : idx;
            key = v;
        }

        /** {@inheritDoc} */
        @Override public long mvccCoordinatorVersion() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public long mvccCounter() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(RowKey.class, this);
        }
    }

    /**
     * Single value row.
     */
    private static final class RowKeyMvcc extends RowKey {
        /** */
        private final MvccCoordinatorVersion mvccVer;

        /**
         * @param key Key.
         * @param mvccVer Mvcc version.
         */
        RowKeyMvcc(Value key, MvccCoordinatorVersion mvccVer) {
            super(key);

            assert mvccVer != null;

            this.mvccVer = mvccVer;
        }

        /** {@inheritDoc} */
        @Override public long mvccCoordinatorVersion() {
            return mvccVer.coordinatorVersion();
        }

        /** {@inheritDoc} */
        @Override public long mvccCounter() {
            return mvccVer.counter();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(RowKeyMvcc.class, this);
        }
    }

    /**
     * Row of two values.
     */
    private static class RowPair extends GridH2SearchRowAdapter  {
        /** */
        private Value v1;

        /** */
        private Value v2;

        /**
         * @param v1 First value.
         * @param v2 Second value.
         */
        private RowPair(Value v1, Value v2) {
            this.v1 = v1;
            this.v2 = v2;
        }

        /** {@inheritDoc} */
        @Override public int getColumnCount() {
            return 2;
        }

        /** {@inheritDoc} */
        @Override public Value getValue(int idx) {
            return idx == 0 ? v1 : v2;
        }

        /** {@inheritDoc} */
        @Override public void setValue(int idx, Value v) {
            if (idx == 0)
                v1 = v;
            else {
                assert idx == 1 : idx;

                v2 = v;
            }
        }

        /** {@inheritDoc} */
        @Override public long mvccCoordinatorVersion() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public long mvccCounter() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(RowPair.class, this);
        }
    }

    /**
     *
     */
    private static final class RowPairMvcc extends RowPair {
        /** */
        private final MvccCoordinatorVersion mvccVer;

        /**
         * @param v1 First value.
         * @param v2 Second value.
         * @param mvccVer Mvcc version.
         */
        RowPairMvcc(Value v1, Value v2, MvccCoordinatorVersion mvccVer) {
            super(v1, v2);

            assert mvccVer != null;

            this.mvccVer = mvccVer;
        }

        /** {@inheritDoc} */
        @Override public long mvccCoordinatorVersion() {
            return mvccVer.coordinatorVersion();
        }

        /** {@inheritDoc} */
        @Override public long mvccCounter() {
            return mvccVer.counter();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(RowPairMvcc.class, this);
        }
    }

    /**
     * Simple array based row.
     */
    private static class RowSimple extends GridH2SearchRowAdapter {
        /** */
        @GridToStringInclude
        private Value[] vals;

        /**
         * @param vals Values.
         */
        private RowSimple(Value[] vals) {
            this.vals = vals;
        }

        /** {@inheritDoc} */
        @Override public int getColumnCount() {
            return vals.length;
        }

        /** {@inheritDoc} */
        @Override public Value getValue(int idx) {
            return vals[idx];
        }

        /** {@inheritDoc} */
        @Override public void setValue(int idx, Value v) {
            vals[idx] = v;
        }

        /** {@inheritDoc} */
        @Override public long mvccCoordinatorVersion() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public long mvccCounter() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(RowSimple.class, this);
        }
    }

    /**
     *
     */
    private static class RowSimpleMvcc extends RowSimple {
        /** */
        private final MvccCoordinatorVersion mvccVer;

        /**
         * @param vals Values.
         * @param mvccVer Mvcc version.
         */
        RowSimpleMvcc(Value[] vals, MvccCoordinatorVersion mvccVer) {
            super(vals);

            assert mvccVer != null;

            this.mvccVer = mvccVer;
        }

        /** {@inheritDoc} */
        @Override public long mvccCoordinatorVersion() {
            return mvccVer.coordinatorVersion();
        }

        /** {@inheritDoc} */
        @Override public long mvccCounter() {
            return mvccVer.counter();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(RowSimpleMvcc.class, this);
        }
    }
}
