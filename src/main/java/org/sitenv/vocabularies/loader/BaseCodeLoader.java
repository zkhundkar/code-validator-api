package org.sitenv.vocabularies.loader;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.StrBuilder;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

/**
 * Created by Brian on 2/6/2016.
 */
public abstract class BaseCodeLoader implements VocabularyLoader{

    public final String codeTableInsertSQLPrefix = "insert into CODES (ID, CODE, DISPLAYNAME, CODESYSTEM, CODESYSTEMOID) values ";
    protected static final int BATCH_SIZE = 100;

    protected String code;
    protected String codeSystem;
    protected String oid;

    public boolean insertCode(String sql, Connection connection) throws SQLException {
        PreparedStatement preparedStatement = null;
        boolean inserted = false;
        if(sql.endsWith(",")){
            sql = StringUtils.chop(sql);
        }
        try{
            preparedStatement = connection.prepareStatement(sql);
            inserted = preparedStatement.execute();
            connection.commit();
        }finally {
            if(preparedStatement != null){
                preparedStatement.close();
            }
        }
       return inserted;
    }

    protected void buildCodeInsertQueryString(StrBuilder insertQueryBuilder, String code, String displayName, String codeSystem, String oid) {
        insertQueryBuilder.append("(");
        insertQueryBuilder.append("DEFAULT");
        insertQueryBuilder.append(",'");
        insertQueryBuilder.append(code.trim().toUpperCase());
        insertQueryBuilder.append("','");
        String shortName = displayName.trim().toUpperCase().replaceAll("'", "''");
        if (shortName.length()>800) {
        	shortName = shortName.substring(0, 796);
        	if (shortName.endsWith("'") && (!shortName.endsWith("''"))) {
        		shortName = shortName + "'..";        	
        	}
        	else {
        		shortName = shortName + " ..";
        	}
        };
        insertQueryBuilder.append(shortName);
        insertQueryBuilder.append("','");
        insertQueryBuilder.append(codeSystem);
        insertQueryBuilder.append("','");
        insertQueryBuilder.append(oid);
        insertQueryBuilder.append("'),");
    }
    
    protected void moveToDone(File file) {
        String sourceFolder = file.getParent();
        File destinationFolder = new File(sourceFolder+"/DONE");
        File destinationFile = new File(sourceFolder+"/DONE/"+file.getName());
        Path destPath = destinationFolder.toPath();
        if (!destinationFolder.exists())
        {
            destinationFolder.mkdirs();
        }

        // Handle case where the source file is a duplicate of one that was already processed
        //if (!destinationFile.exists() )
        //{
            try {            	
            	System.out.println("Moving "+file.getAbsolutePath() +" to "+destinationFile.getAbsolutePath() );
            	Files.move(file.toPath(), destPath.resolve(file.getName()), StandardCopyOption.REPLACE_EXISTING);
            }
            catch (IOException e) {
            	System.out.print(e.toString());
            	e.printStackTrace();
            }
            //String result = " succeeded.";
            //if (!file.renameTo(destinationFile)) {
            //	result = " failed.";
            //}
        //}
        //else
        //{
        //    System.out.println(destinationFile.getAbsolutePath() + "  already exists - deleting source");
        //    file.delete();
        //}    	
    }
    
}
