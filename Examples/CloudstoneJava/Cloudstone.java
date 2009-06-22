
package cloudstone;

import deploylib.*;
import org.json.JSONObject;
import scala.*;

public class Cloudstone {
    public void run(int count, String type) {
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
        
        Object[] railsSettings   = {count, type};
        
        Object[] mysqlSettings   = {1, "c1.xlarge"};
        Object[] haproxySettings = {1, "m1.small"};
        Object[] nginxSettings   = {1, "m1.small"};
        Object[] fabanSettings   = {1, "c1.xlarge"};
        
    }
    
    private InstanceGroup runInstances(int count, String typeString) {
        Enumeration.Value type = Instance$.getValue(typeString);
    }
}

