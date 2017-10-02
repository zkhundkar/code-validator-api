package org.sitenv.vocabularies.loader.code;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.StrBuilder;
import org.apache.log4j.Logger;
import org.sitenv.vocabularies.loader.BaseCodeLoader;
import org.sitenv.vocabularies.loader.VocabularyLoader;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * Created by Brian on 2/7/2016.
 */
@Component(value = "RXNORM")
public class RxNormLoader extends BaseCodeLoader implements VocabularyLoader {
    private static Logger logger = Logger.getLogger(RxNormLoader.class);

    @Override
    public void load(List<File> filesToLoad, Connection connection) {
        FileReader fileReader = null;
        BufferedReader br = null;
        boolean okToMove = false;
        File thisFile = null;
        try {
            StrBuilder insertQueryBuilder = new StrBuilder(codeTableInsertSQLPrefix);
            int totalCount = 0, pendingCount = 0;

            for (File file : filesToLoad) {
                if (file.isFile() && !file.isHidden()) {
                    logger.debug("Loading RxNorm File: " + file.getName());
                    String codeSystem = file.getParentFile().getName();
                    fileReader = new FileReader(file);
                    br = new BufferedReader(fileReader);
                    String available;
                    thisFile = file;
                    okToMove = false;
                    while ((available = br.readLine()) != null) {
                        String[] line = StringUtils.splitPreserveAllTokens(available, "|", 16);
                        String code = line[0];
                        String displayName = StringUtils.strip(line[14], "\\");

                        buildCodeInsertQueryString(insertQueryBuilder, code, displayName, codeSystem, CodeSystemOIDs.RXNORM.codesystemOID());
                        pendingCount++;
                        if ((++totalCount % BATCH_SIZE) == 0) {
                            insertCode(insertQueryBuilder.toString(), connection);
                            insertQueryBuilder.clear();
                            insertQueryBuilder.append(codeTableInsertSQLPrefix);
                            pendingCount = 0;
                        }
                    }
                    okToMove = true;  // Move file to archive folder
                    logger.info("Loaded " + Integer.toString(totalCount) + " codes.");
                }
            }
            if (pendingCount > 0) {
                insertCode(insertQueryBuilder.toString(), connection);
            }
        } catch (IOException e) {
            logger.error(e);
        } catch (SQLException e) {
            logger.error(e);        	
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    fileReader.close();
                    br.close();
                    if (okToMove) {
                    	moveToDone(thisFile);  // Move file to archive folder
                    	logger.info("Moved " + thisFile.getName() + " to DONE folder");                    	
                    }
                } catch (IOException e) {
                    logger.error(e);
                }
            }
        }
    }
}
