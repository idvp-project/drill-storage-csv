package org.apache.drill.exec.store.csv;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.exec.store.easy.text.TextFormatPlugin;


@JsonTypeName("idvp-csv")
@JsonIgnoreProperties(ignoreUnknown = true)
class CsvFormatConfig extends TextFormatPlugin.TextFormatConfig {
}
