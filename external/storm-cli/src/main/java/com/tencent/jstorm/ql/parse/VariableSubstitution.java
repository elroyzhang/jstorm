package com.tencent.jstorm.ql.parse;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.tencent.jstorm.ql.processors.SetProcessor;
import com.tencent.jstorm.ql.session.SessionState;

public class VariableSubstitution {

  private static final Log l4j = LogFactory.getLog(VariableSubstitution.class);
  protected static Pattern varPat = Pattern
      .compile("\\$\\{[^\\}\\$\u0020]+\\}");

  private String getSubstitute(Map conf, String var) {
    String val = null;
    try {
      if (var.startsWith(SetProcessor.SYSTEM_PREFIX)) {
        val =
            System.getProperty(var.substring(SetProcessor.SYSTEM_PREFIX
                .length()));
      }
    } catch (SecurityException se) {
      l4j.warn("Unexpected SecurityException in Configuration", se);
    }
    if (val == null) {
      if (var.startsWith(SetProcessor.ENV_PREFIX)) {
        val = System.getenv(var.substring(SetProcessor.ENV_PREFIX.length()));
      }
    }
    if (val == null) {
      if (var.startsWith(SetProcessor.HIVECONF_PREFIX)) {
        // val = conf.get(var.substring(SetProcessor.HIVECONF_PREFIX.length()));
      }
    }
    if (val == null) {
      if (var.startsWith(SetProcessor.HIVEVAR_PREFIX)) {
        val =
            SessionState.get().getStormVariables()
                .get(var.substring(SetProcessor.HIVEVAR_PREFIX.length()));
      } else {
        val = SessionState.get().getStormVariables().get(var);
      }
    }
    return val;
  }

  public String substitute(Map conf, String expr) {

    if (expr == null) {
      return null;
    }
    Matcher match = varPat.matcher("");
    String eval = expr;
    for (int s = 0; s < 20; s++) {
      match.reset(eval);
      if (!match.find()) {
        return eval;
      }
      String var = match.group();
      var = var.substring(2, var.length() - 1); // remove ${ .. }
      String val = getSubstitute(conf, var);

      if (val == null) {
        l4j.debug("Interpolation result: " + eval);
        return eval; // return literal, no substitution found
      }
      // substitute
      eval =
          eval.substring(0, match.start()) + val + eval.substring(match.end());
    }
    throw new IllegalStateException("Variable substitution depth too large: "
        + "" + " " + expr);
  }
}
