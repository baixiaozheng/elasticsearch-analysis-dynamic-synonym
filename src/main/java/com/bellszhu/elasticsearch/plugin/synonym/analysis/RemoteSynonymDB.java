package com.bellszhu.elasticsearch.plugin.synonym.analysis;

import com.bellszhu.elasticsearch.plugin.DynamicSynonymPlugin;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.synonym.SolrSynonymParser;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.analysis.synonym.WordnetSynonymParser;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.env.Environment;

import java.io.*;
import java.nio.file.Path;
import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;

public class RemoteSynonymDB implements SynonymFile{

    // 配置文件名
    private final static String DB_PROPERTIES = "jdbc.properties";
    private static Logger logger = LogManager.getLogger("dynamic-synonym");
    private String format;

    private boolean expand;
    private boolean lenient;

    private Analyzer analyzer;

    private Environment env;

    // 数据库配置
    private String location;

    private long lastModified;

    private Connection connection = null;

    private Statement statement = null;

    private Properties props;

    private Path conf_dir;

    RemoteSynonymDB(Environment env, Analyzer analyzer,
                        boolean expand, boolean lenient, String format, String location) {
        this.analyzer = analyzer;
        this.expand = expand;
        this.format = format;
        this.env = env;
        this.location = location;
        this.lenient = lenient;
        this.props = new Properties();

        //读取当前 jar 包存放的路径
        Path filePath = PathUtils.get(new File(DynamicSynonymPlugin.class.getProtectionDomain().getCodeSource()
                        .getLocation().getPath())
                        .getParent(), "/")
                .toAbsolutePath();
        this.conf_dir = filePath.resolve(DB_PROPERTIES);

        //判断文件是否存在
        File configFile = conf_dir.toFile();
        InputStream input = null;
        try {
            input = new FileInputStream(configFile);
        } catch (FileNotFoundException e) {
            logger.info("jdbc-reload.properties not find. " + e);
        }
        if (input != null) {
            try {
                props.load(input);
            } catch (IOException e) {
                logger.error("fail to load the jdbc-reload.properties," + e);
            }
        }
        isNeedReloadSynonymMap();
    }
    /**
     * 加载同义词词典至SynonymMap中
     * @return SynonymMap
     */
    @Override
    public SynonymMap reloadSynonymMap() {
        try {
            logger.info("start reload dbRemote synonym from {}.", location);
            Reader rulesReader = getReader();
            SynonymMap.Builder parser = null;
            if ("wordnet".equalsIgnoreCase(format)) {
                parser = new WordnetSynonymParser(true, expand, analyzer);
                ((WordnetSynonymParser) parser).parse(rulesReader);
            } else {
                parser = new SolrSynonymParser(true, expand, analyzer);
                ((SolrSynonymParser) parser).parse(rulesReader);
            }
            return parser.build();
        } catch (Exception e) {
            logger.error("reload dbRemote synonym {} error!", e, location);
            throw new IllegalArgumentException(
                    "could not reload dbRemote synonyms file to build synonyms", e);
        }

    }

    /**
     * 判断是否需要进行重新加载
     * @return true or false
     */
    @Override
    public boolean isNeedReloadSynonymMap() {
        try {
            Long lastModify = getLastModify();
            if (lastModified < lastModify) {
                lastModified = lastModify;
                return true;
            }
        } catch (Exception e) {
            logger.error(e);
        }

        return false;
    }

    /**
     * 获取同义词库最后一次修改的时间
     * 用于判断同义词是否需要进行重新加载
     *
     * @return getLastModify
     */
    public Long getLastModify() {
        ResultSet resultSet = null;
        Long last_modify_long = null;
        try {
            if (connection == null || statement == null) {
                Class.forName(props.getProperty("jdbc.driver"));
                connection = DriverManager.getConnection(
                        props.getProperty("jdbc.url"),
                        props.getProperty("jdbc.user"),
                        props.getProperty("jdbc.password")
                );
                statement = connection.createStatement();
            }
            resultSet = statement.executeQuery(props.getProperty("jdbc.lastModified.synonym.sql"));
            while (resultSet.next()) {
                Timestamp last_modify_dt = resultSet.getTimestamp("update_time");
                last_modify_long = last_modify_dt.getTime();
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }

        }
        return last_modify_long;
    }
    /**
     * 查询数据库中的同义词
     * @return DBData
     */
    public ArrayList<String> getDBData() {
        ArrayList<String> arrayList = new ArrayList<>();
        ResultSet resultSet = null;
        try {
            if (connection == null || statement == null) {
                Class.forName(props.getProperty("jdbc.driver"));
                connection = DriverManager.getConnection(
                        props.getProperty("jdbc.url"),
                        props.getProperty("jdbc.user"),
                        props.getProperty("jdbc.password")
                );
                statement = connection.createStatement();
            }
            resultSet = statement.executeQuery(props.getProperty("jdbc.reload.synonym.sql"));
            while (resultSet.next()) {
                String theWord = resultSet.getString("words");
                arrayList.add(theWord);
            }
        } catch (ClassNotFoundException e) {
            logger.error(e);
        } catch (SQLException e) {
            logger.error(e);
        } finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }

        }
        return arrayList;
    }

    /**
     * 同义词库的加载
     * @return Reader
     */
    @Override
    public Reader getReader() {

        StringBuffer sb = new StringBuffer();
        try {
            ArrayList<String> dbData = getDBData();
            for (int i = 0; i < dbData.size(); i++) {
                logger.info("load the synonym from db," + dbData.get(i));
                sb.append(dbData.get(i))
                        .append(System.getProperty("line.separator"));
            }
        } catch (Exception e) {
            logger.error("reload synonym from db failed");
        }
        return new StringReader(sb.toString());
    }

}
