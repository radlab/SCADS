
package CloudstoneJava;

import deploylib.*;
import org.json.JSONObject;
import org.json.JSONArray;
import scala.*;

public class Cloudstone {
    public void run(int count, String type) throws org.json.JSONException {
        /**
         * This is a simple example where you pass on the command line
         * the number and size of rails servers you would like in the 
         * following form:
         *
         * scala cloudstone --count 2 --type c1.xlarge
         *
         * The stack will have the following defaults for the other roles:
         * 1 MySQL server on an c1.xlarge instance
         * 1 HAProxy server on a m1.small instance
         * 1 nginx server on a m1.small instance
         * 1 Faban master/driver server on a c1.xlarge
         */
        
        Object[] railsSettings   = {count, type, InstanceType.cores(type) * 2};
        
        Object[] mysqlSettings   = {1, "c1.xlarge"};
        Object[] haproxySettings = {1, "m1.small"};
        Object[] nginxSettings   = {1, "m1.small"};
        Object[] fabanSettings   = {1, "c1.xlarge"};
        
        
        InstanceGroup rails = runInstances((Integer)railsSettings[0],
            (String)railsSettings[1]);
                                           
        InstanceGroup mysql = runInstances((Integer)mysqlSettings[0],
            (String)mysqlSettings[1]);
        InstanceGroup haproxy = runInstances((Integer)haproxySettings[0],
            (String)haproxySettings[1]);
        InstanceGroup nginx = runInstances((Integer)nginxSettings[0],
            (String)nginxSettings[1]);
        InstanceGroup faban = runInstances((Integer)fabanSettings[0],
            (String)fabanSettings[1]);
            
        InstanceGroup allInstances = new InstanceGroup();
        allInstances.addAll(rails);
        allInstances.addAll(mysql);
        allInstances.addAll(haproxy);
        allInstances.addAll(nginx);
        allInstances.addAll(faban);
        
        
        /**************************************************
         * Don't forget to wait on the instances          *
         **************************************************/
        allInstances.parallelMap(new WaitUntilReady());

        /* Rails Configuration */
        JSONObject railsConfig = new JSONObject();
        railsConfig.put("recipes", new JSONArray().put("cloudstone::rails"));
        JSONObject railsRails = new JSONObject();

        JSONObject railsRailsPorts = new JSONObject();
        railsRailsPorts.put("start", 3000);
        railsRailsPorts.put("count", (Integer)railsSettings[2]);
        railsRails.put("ports", railsRailsPorts);

        JSONObject railsRailsDatabase = new JSONObject();
        railsRailsDatabase.put("host", mysql.getFirst().privateDnsName());
        railsRailsDatabase.put("adapter", "mysql");
        railsRails.put("database", railsRailsDatabase);

        JSONObject railsRailsMemcached = new JSONObject();
        railsRailsMemcached.put("host", "localhost");
        railsRailsMemcached.put("port", 1211);
        railsRails.put("memcached", railsRailsMemcached);

        JSONObject railsRailsGeocoder = new JSONObject();
        railsRailsGeocoder.put("host", faban.getFirst().privateDnsName());
        railsRailsGeocoder.put("port", 9980);
        railsRails.put("geocoder", railsRailsGeocoder);

        railsConfig.put("rails", railsRails);

        /* mysql configuration */
        JSONObject mysqlConfig = new JSONObject();
        JSONArray mysqlRecipes = new JSONArray();
        mysqlRecipes.put("cloudstone::mysql");
        mysqlRecipes.put("cloudstone::faban-agent");
        mysqlConfig.put("recipes", mysqlRecipes);
        JSONObject mysqlMysql = new JSONObject();

        mysqlMysql.put("server_id", 1);
        mysqlConfig.put("mysql", mysqlMysql);

        JSONObject mysqlFaban = new JSONObject();
        mysqlFaban.put("mysql", true);
        mysqlConfig.put("faban", mysqlFaban);

        /* haproxy configuration */
        JSONObject haproxyConfig = new JSONObject();
        haproxyConfig.put("recipes", new JSONArray().put("cloudstone::haproxy"));
        JSONObject haproxyHaproxy = new JSONObject();
        haproxyHaproxy.put("port", 4000);

        JSONObject haproxyHaproxyServers = new JSONObject();
        for (Instance instance : rails) {
            JSONObject server = new JSONObject();
            server.put("start", 3000);
            server.put("count", (Integer)railsSettings[2]);
            haproxyHaproxyServers.put(instance.privateDnsName(), server);
        }
        haproxyHaproxy.put("servers", haproxyHaproxyServers);

        haproxyConfig.put("haproxy", haproxyHaproxy);

        /* nginx configuration */
        JSONObject nginxConfig = new JSONObject();
        JSONArray nginxRecipes = new JSONArray();
        nginxRecipes.put("cloudstone::nginx");
        nginxRecipes.put("cloudstone::faban-agent");
        nginxConfig.put("recipes", nginxRecipes);
        JSONObject nginxNginx = new JSONObject();
        JSONObject nginxNginxServers = new JSONObject();

        for (Instance instance : haproxy) {
            JSONObject server = new JSONObject();
            server.put("start", 4000);
            server.put("count", 1);
            nginxNginxServers.put(instance.privateDnsName(), server);
        }
        nginxNginx.put("servers", nginxNginxServers);
        nginxConfig.put("nginx", nginxNginx);

        JSONObject nginxFaban = new JSONObject();
        nginxFaban.put("mysql", false);
        nginxConfig.put("faban", nginxFaban);

        /* faban configuration */
        JSONObject fabanConfig = new JSONObject();
        fabanConfig.put("recipes", new JSONArray().put("cloudstone::faban"));
        JSONObject fabanFaban = new JSONObject();
        JSONObject fabanFabanHosts = new JSONObject();
        fabanFabanHosts.put("driver", faban.getFirst().privateDnsName());
        fabanFabanHosts.put("webserver", nginx.getFirst().privateDnsName());
        fabanFabanHosts.put("database", mysql.getFirst().privateDnsName());
        fabanFabanHosts.put("storage", "");
        fabanFabanHosts.put("cache", "");

        fabanFaban.put("hosts", fabanFabanHosts);
        fabanConfig.put("faban", fabanFaban);
        
        System.out.println("Deploying mysql.");
        mysql.parallelMap(new Deploy(mysqlConfig));
        System.out.println("Deploying rails.");
        rails.parallelMap(new Deploy(railsConfig));
        System.out.println("Deploying haproxy.");
        haproxy.parallelMap(new Deploy(haproxyConfig));
        System.out.println("Deploying nginx.");
        nginx.parallelMap(new Deploy(nginxConfig));
        System.out.println("Deploying faban.");
        faban.parallelMap(new Deploy(fabanConfig));
        
        System.out.println("All done.");
        
    }
    
    private InstanceGroup runInstances(int count, String typeString) {
        String imageId = InstanceType.bits(typeString).equals("32-bit") ?
                                    "ami-e7a2448e" : "ami-e4a2448d";
        String keyName = "abeitch";
        String keyPath = "/Users/aaron/.ec2/id_rsa-abeitch";
        String location = "us-east-1a";
        
        return DataCenter.runInstances(imageId, count, keyName, keyPath,
                                       typeString, location);
    }
    
    private class WaitUntilReady implements InstanceExecute {
        public void execute(Instance instance) {
            instance.waitUntilReady();
        }
    }
    
    private class Deploy implements InstanceExecute {
        private JSONObject config;
        
        public Deploy(JSONObject config) {
            this.config = config;
        }
        
        public void execute(Instance instance) {
            instance.deploy(config);
        }
    }
}

