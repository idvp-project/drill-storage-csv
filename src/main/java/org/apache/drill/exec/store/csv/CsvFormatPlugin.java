package org.apache.drill.exec.store.csv;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.RecordWriter;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.easy.EasyFormatPlugin;
import org.apache.drill.exec.store.dfs.easy.EasyWriter;
import org.apache.drill.exec.store.dfs.easy.FileWork;
import org.apache.drill.exec.store.easy.text.compliant.CsvRecordReader;
import org.apache.drill.exec.store.easy.text.compliant.TextParsingSettings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class CsvFormatPlugin extends EasyFormatPlugin<CsvFormatConfig> {

    private static final boolean IS_COMPRESSIBLE = false;
    private static final String DEFAULT_NAME = "csv";

    public CsvFormatPlugin(String name, DrillbitContext context, Configuration fsConf, StoragePluginConfig storageConfig) {
        super(name, context, fsConf, storageConfig,
                new CsvFormatConfig(), true, false, true, true,
                Collections.emptyList(), DEFAULT_NAME);
    }

    public CsvFormatPlugin(String name, DrillbitContext context, Configuration fsConf, StoragePluginConfig config,
                           CsvFormatConfig formatPluginConfig) {
        super(name, context, fsConf, config, formatPluginConfig, true, false, true, true,
                Collections.singletonList("csv"), DEFAULT_NAME);
    }


    @Override
    public boolean supportsPushDown() {
        return true;
    }

    @Override
    public RecordReader getRecordReader(FragmentContext context,
                                        DrillFileSystem dfs,
                                        FileWork fileWork,
                                        List<SchemaPath> columns,
                                        String userName) throws ExecutionSetupException {

        Path path = dfs.makeQualified(new Path(fileWork.getPath()));
        FileSplit split = new FileSplit(path, fileWork.getStart(), fileWork.getLength(), new String[] { "" });
        TextParsingSettings settings = new TextParsingSettings();
        settings.set(getConfig());
        return new CsvRecordReader(split, dfs, settings, columns);
    }

    @Override
    public RecordWriter getRecordWriter(FragmentContext context, EasyWriter writer) throws IOException {
        return null;
    }

    @Override
    public int getReaderOperatorType() {
        return 0;
    }

    @Override
    public int getWriterOperatorType() {
        throw new UnsupportedOperationException();
    }
}
