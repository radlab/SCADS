/**
 * A logger wrapper, currently under development.
 * Non-UCB affiliates should check with Ari Rabkin (asrabkin@gmail) before using
 * 
 */
package edu.berkeley;
import org.apache.log4j.Level;

public final class Log2 {
    
  public static void trace(org.apache.log4j.Logger log, Object... s) {
    if(log.isTraceEnabled()) {
      StringBuilder sb = new StringBuilder();
      for(int i=0; i < s.length -1; ++i) 
        sb.append(s[i]);
      Object lastItem = s[s.length -1];
      if(lastItem instanceof Throwable)
        log.trace(sb.toString(), (Throwable) lastItem);
      else {
        sb.append(lastItem);
        log.trace(sb.toString());
      }
    } 
  }

  public static void debug(org.apache.log4j.Logger log, Object... s) {
    if(log.isDebugEnabled()) {
      StringBuilder sb = new StringBuilder();
      for(int i=0; i < s.length -1; ++i) 
        sb.append(s[i]);
      Object lastItem = s[s.length -1];
      if(lastItem instanceof Throwable)
        log.warn(sb.toString(), (Throwable) lastItem);
      else {
        sb.append(lastItem);
        log.debug(sb.toString());
      }
    } 
  }

  public static void info(org.apache.log4j.Logger log, Object... s) {
    if(log.isInfoEnabled()) {
      StringBuilder sb = new StringBuilder();
      for(int i=0; i < s.length -1; ++i) 
        sb.append(s[i]);
      Object lastItem = s[s.length -1];
      if(lastItem instanceof Throwable)
        log.warn(sb.toString(), (Throwable) lastItem);
      else {
        sb.append(lastItem);
        log.info(sb.toString());
      }
    } 
  }
  
  public static void warn(org.apache.log4j.Logger log, Object... s) {
    if(log.isEnabledFor(Level.WARN)) {
      StringBuilder sb = new StringBuilder();
      for(int i=0; i < s.length -1; ++i) 
        sb.append(s[i]);
      Object lastItem = s[s.length -1];
      if(lastItem instanceof Throwable)
        log.warn(sb.toString(), (Throwable) lastItem);
      else {
        sb.append(lastItem);
        log.warn(sb.toString());
      }
    } 
 }
  
  public static void error(org.apache.log4j.Logger log, Object... s) {
    if(log.isEnabledFor(Level.ERROR)) {
      StringBuilder sb = new StringBuilder();
      for(int i=0; i < s.length -1; ++i) 
        sb.append(s[i]);
      Object lastItem = s[s.length -1];
      if(lastItem instanceof Throwable)
        log.warn(sb.toString(), (Throwable) lastItem);
      else {
        sb.append(lastItem);
        log.error(sb.toString());
      }
    } 
  }
  

  public static boolean isLoggerCall(String name) {
    return name.equals("info") || name.equals("debug") || name.equals("warn") || name.equals("error")
    || name.equals("trace") || name.equals("fatal");
  }
}

