

import javax.annotation.Resources;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;


public class FileRender {

    protected ArrayList<String> syncTables = new ArrayList<String>();

    protected String sourceSchema = null;
    protected String targetSchema = null;
    protected String sourceDbHost = null;
    protected String targetDbHost = null;

    private final  String propertiseFile = System.getProperty("user.dir")+ "/SyncTable.properties";
    private final  String resourcePropertiseFile = this.getClass().getClassLoader().getResource("sample/SyncTable.properties").getFile();


    protected void ReadSyncTablePropertise() {

        String currentProperty = "empty";




        try {
            File file = new File(propertiseFile);
            if (!file.exists()) {

                Files.move(Paths.get(resourcePropertiseFile),Paths.get(propertiseFile), StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }




        try (BufferedReader br = new BufferedReader(new FileReader(propertiseFile))) {

            String sCurrentLine;
            System.out.println("\nReading SyncTable.properties file ....\n");

            while ((sCurrentLine = br.readLine()) != null) {

                if (!sCurrentLine.startsWith("#") && !sCurrentLine.equals("")){


                    if (sCurrentLine.startsWith("@")){

                        currentProperty = sCurrentLine.substring(2);
                    }

                    else if (currentProperty.contentEquals("SOURCE_DB_HOST")){

                        sourceDbHost = sCurrentLine;
                        System.out.println("SOURCE_DB_HOST : "+sourceDbHost);

                    }
                    else if (currentProperty.contentEquals("SOURCE_DB_SCHEMA_NAME")){

                        sourceSchema = sCurrentLine;
                        System.out.println("SOURCE_DB_SCHEMA_NAME : "+sourceSchema);

                    }
                    else if (currentProperty.contentEquals("TARGET_DB_HOST")){

                        targetDbHost = sCurrentLine;
                        System.out.println("TARGET_DB_HOST : "+targetDbHost);

                    }
                    else if (currentProperty.contentEquals("TARGET_DB_SCHEMA_NAME")){


                        targetSchema = sCurrentLine;
                        System.out.println("TARGET_DB_SCHEMA_NAME : "+targetSchema+"\n");

                    }
                    else if (currentProperty.contentEquals("TABLES_ENBALE_FOR_SYNC")){

                        syncTables.add (sCurrentLine);
                        System.out.println("TABLES_ENBALE_FOR_SYNC : "+sCurrentLine);

                    }

                }

            }

            System.out.println("\n");

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
