
package JavaCloudstoneExample;

import deploylib.*;
import org.json.JSONObject;
import org.json.JSONArray;
import scala.*;
import java.util.Map;
import java.util.HashMap;

/**
* Currently errors because the the mysql connector is not being installed.
* just need to add a field to the mysql faban agent.
*/

public class Colocation {
    public void run() throws org.json.JSONException {
        /**
         * This is a simple example of deploying Cloudstone and Faban.
         *
         * The stack will have the following defaults:
         * 1 MySQL server on an c1.xlarge instance
         * 1 HAProxy server on a m1.small instance
         * 1 nginx server on a m1.small instance
         * 1 Faban master/driver server on a c1.xlarge
         */

        
        HashMap<String, Object> thinCount = new HashMap<String, Object>();
        thinCount.put("thins", 16);
        
        String[] zero = {"mysql", "faban-agent"};
        String[] one = {"rails"};
        String[] two = {"haproxy"};
        String[] three = {"nginx", "faban-agent"};
        String[] four = {"faban"};
        
        InstanceConfig[] instanceConfigs = {
            new InstanceConfig(1, "c1.xlarge", zero, null),
            new InstanceConfig(2, "c1.xlarge", one, thinCount),
            new InstanceConfig(1, "m1.small", two, null),
            new InstanceConfig(1, "m1.small", three, null),
            new InstanceConfig(1, "c1.xlarge", four, null)
        };
        
        int[] mysqlIndices = {0};
        int[] railsIndices = {1};
        int[] haproxyIndices = {2};
        int[] nginxIndices = {3};
        int[] fabanIndices = {4};
        
        InstanceGroup[] instances = new InstanceGroup[instanceConfigs.length];
        
        
        System.out.println("Requesting instances.");
        for (int i = 0; i < instanceConfigs.length; i ++) {
            instances[i] = runInstances(instanceConfigs[i]);
        }
        System.out.println("Instances gotten.");
        
        InstanceGroup allInstances = new InstanceGroup(instances);
        
        
        /**************************************************
         * Don't forget to wait on the instances          *
         **************************************************/
        System.out.println("Waiting on instances.");
        allInstances.parallelMap(new WaitUntilReady());
        System.out.println("Instances ready.");
        
        /* Rails Configuration */
        JSONObject[] railsObject = new JSONObject[railsIndices.length];
        
        int j = 0;
        for (int index : railsIndices) {
            railsObject[j] = new JSONObject();
            
            JSONObject railsObjectPorts = new JSONObject();
            railsObjectPorts.put("start", 3000);
            railsObjectPorts.put("count", ((Integer)instanceConfigs[index].
                getSettings().get("thins")).intValue());
            railsObject[j].put("ports", railsObjectPorts);
            
            JSONObject railsObjectDatabase = new JSONObject();
            railsObjectDatabase.put("host", instances[mysqlIndices[0]].
                getFirst().privateDnsName());
            railsObjectDatabase.put("adapter", "mysql");
            railsObject[j].put("database", railsObjectDatabase);
            
            JSONObject railsObjectMemcached = new JSONObject();
            railsObjectMemcached.put("host", "localhost");
            railsObjectMemcached.put("port", 1211);
            railsObject[j].put("memcached", railsObjectMemcached);
            
            JSONObject railsObjectGeocoder = new JSONObject();
            railsObjectGeocoder.put("host", instances[fabanIndices[0]].
                getFirst().privateDnsName());
            railsObjectGeocoder.put("port", 9980);
            railsObject[j].put("geocoder", railsObjectGeocoder);
            
            j++;
        }

        /* mysql configuration */
        JSONObject mysqlObject = new JSONObject();
        mysqlObject.put("server_id", 1);

        JSONObject mysqlFabanAgent = new JSONObject();
        mysqlFabanAgent.put("mysql", true);

        /* haproxy configuration */
        JSONObject haproxyObject = new JSONObject();
        haproxyObject.put("port", 4000);

        JSONObject haproxyObjectServers = new JSONObject();
        for (int index : railsIndices) {
            for (Instance instance : instances[index]) {
                JSONObject server = new JSONObject();
                server.put("start", 3000);
                server.put("count", ((Integer)instanceConfigs[index].
                    getSettings().get("thins")).intValue());
                haproxyObjectServers.put(instance.privateDnsName(), server);
            }
        }
        haproxyObject.put("servers", haproxyObjectServers);

        /* nginx configuration */
        JSONObject nginxObject = new JSONObject();
        JSONObject nginxObjectServers = new JSONObject();

        for (int index : haproxyIndices) {
            for (Instance instance : instances[index]) {
                JSONObject server = new JSONObject();
                server.put("start", 4000);
                server.put("count", 1);
                nginxObjectServers.put(instance.privateDnsName(), server);
            }
        }
        nginxObject.put("servers", nginxObjectServers);

        JSONObject nonMysqlFabanAgent = new JSONObject();
        nonMysqlFabanAgent.put("mysql", false);

        /* faban configuration */
        JSONObject fabanObject = new JSONObject();
        JSONObject fabanObjectHosts = new JSONObject();
        fabanObjectHosts.put("driver", instances[fabanIndices[0]].
            getFirst().privateDnsName());
        fabanObjectHosts.put("webserver", instances[nginxIndices[0]].
            getFirst().privateDnsName());
        fabanObjectHosts.put("database", instances[mysqlIndices[0]].
            getFirst().privateDnsName());
        fabanObjectHosts.put("storage", "");
        fabanObjectHosts.put("cache", "");

        fabanObject.put("hosts", fabanObjectHosts);
        
        /* Chukwa config */
        JSONObject chukwaObject = new JSONObject();
        JSONArray chukwaAdapters = new JSONArray();
        chukwaAdapters.put("add org.apache.hadoop.chukwa.datacollection.adaptor.ExecAdaptor Top 15000 /usr/bin/top -b -n 1 -c 0");
        chukwaAdapters.put("add org.apache.hadoop.chukwa.datacollection.adaptor.ExecAdaptor Df 60000 /bin/df -x nfs -x none 0");
        chukwaAdapters.put("add org.apache.hadoop.chukwa.datacollection.adaptor.ExecAdaptor Sar 1000 /usr/bin/sar -q -r -n ALL 55 0");
        chukwaAdapters.put("add org.apache.hadoop.chukwa.datacollection.adaptor.ExecAdaptor Iostat 1000 /usr/bin/iostat -x -k 55 2 0");
        chukwaObject.put("adapters", chukwaAdapters);
        
        for (int i = 0; i < instances.length; i++) {
            System.out.println("Deploying instances: " + i);
            JSONObject config = new JSONObject();
            JSONArray recipes = new JSONArray();
            boolean mysqlPresent = instanceConfigs[i].hasService("mysql");
            for (String service : instanceConfigs[i].getServices()) {
                if (service.equals("mysql")) {
                    recipes.put("cloudstone::mysql");
                    config.put("mysql", mysqlObject);
                }
                if (service.equals("rails")) {
                    recipes.put("cloudstone::rails");
                    config.put("rails", railsObject);
                }
                if (service.equals("haproxy")) {
                    recipes.put("cloudstone::haproxy");
                    config.put("haproxy", haproxyObject);
                }
                if (service.equals("nginx")) {
                    recipes.put("cloudstone::nginx");
                    config.put("nginx", nginxObject);
                }
                if (service.equals("faban")) {
                    recipes.put("cloudstone::faban");
                    config.put("faban", fabanObject);
                }
                if (service.equals("faban-agent")) {
                    recipes.put("cloudstone::faban-agent");
                    config.put("faban", mysqlPresent ? mysqlFabanAgent : nonMysqlFabanAgent);
                }
                if (service.equals("chukwa")) {
                    recipes.put("chukwa::default");
                    config.put("chukwa", chukwaObject);
                }
            }
            config.put("recipes", recipes);
            instances[i].parallelMap(new Deploy(config));
        }
        
        System.out.println("All done.");
        
    }
    
    private InstanceGroup runInstances(InstanceConfig config) {
        String typeString = config.getInstanceType();
        int count = config.getCount();
        
        return DataCenter.runInstances(count, typeString);
    }
    
    private class InstanceConfig {
        private int count;
        private String instanceType;
        private String[] services;
        private Map<String, Object> settings;
        
        public InstanceConfig(int count, String instanceType,
            String[] services, Map<String, Object> settings) {
            this.count = count;
            this.instanceType = instanceType;
            this.services = services;
            this.settings = settings;
        }
        
        public int getCount() { return count; }
        public String getInstanceType() { return instanceType; }
        public String[] getServices() { return services; }
        public Map<String, Object> getSettings() { return settings; }
        
        public boolean hasService(String service) {
            return contains(service, services);
        }
        
        private boolean contains(String text, String[] array) {
            for (int i = 0; i < array.length; i++) {
                if (array[i].equals(text)) return true;
            }
        return false;
        }
    }
    
    private class WaitUntilReady implements InstanceExecute {
        public Object execute(Instance instance) {
            instance.waitUntilReady();
            return null;
        }
    }
    
    private class Tag implements InstanceExecute {
        private String tag;
        public Tag(String tag) { this.tag = tag; }
        public Object execute(Instance instance) {
            instance.tagWith(tag);
            return null;
        }
    }
    
    private class Deploy implements InstanceExecute {
        private JSONObject config;
        
        public Deploy(JSONObject config) {
            this.config = config;
        }
        
        public Object execute(Instance instance) {
            instance.deploy(config);
            return null;
        }
    }
}

