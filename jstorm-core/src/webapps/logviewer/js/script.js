/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
$(function() {
  $(".js-only").show();
});

// Add in custom sorting for some data types
$.extend($.fn.dataTableExt.oSort, {
  "time-str-pre" : function(raw) {
    var s = $(raw).text();
    if (s == "") {
      s = raw;
    }
    if (s.search('All time') != -1) {
      return 1000000000;
    }
    var total = 0;
    $.each(s.split(' '), function(i, v) {
      var amt = parseInt(v);
      if (v.search('ms') != -1) {
        total += amt;
      } else if (v.search('s') != -1) {
        total += amt * 1000;
      } else if (v.search('m') != -1) {
        total += amt * 1000 * 60;
      } else if (v.search('h') != -1) {
        total += amt * 1000 * 60 * 60;
      } else if (v.search('d') != -1) {
        total += amt * 1000 * 60 * 60 * 24;
      }
    });
    return total;
  },
  "time-str-asc" : function(a, b) {
    return ((a < b) ? -1 : ((a > b) ? 1 : 0));
  },
  "time-str-desc" : function(a, b) {
    return ((a < b) ? 1 : ((a > b) ? -1 : 0));
  }
});

function dtAutoPage(selector, conf) {
  if ($(selector.concat(" tr")).length <= 20) {
    $.extend(conf, {
      paging : false
    });
  }
  return $(selector).DataTable(conf);
}
function toggleSys() {
  var sys = $.cookies.get('sys') || false;
  sys = !sys;

  var exDate = new Date();
  exDate.setDate(exDate.getDate() + 365);

  $.cookies.set('sys', sys, {
    'path' : '/',
    'expiresAt' : exDate.toUTCString()
  });
  window.location = window.location;
}

function ensureInt(n) {
  var isInt = /^\d+$/.test(n);
  if (!isInt) {
    alert("'" + n + "' is not integer.");
  }

  return isInt;
}

function sendRequest(id, action, extra, body, cb){
  var opts = {
       type:'POST',
       url: proxyPath() + 'api/v1/topology/' + action + "?tid=" + id,
       dataType : "json"
   };

   if (body) {
       opts.data = JSON.stringify(body);
       opts.contentType = 'application/json; charset=utf-8';
   }
   if (extra != null && extra != "") {
       opts.url += extra;  
   }

   $.ajax(opts).always(function(data){
       cb (data);
   }).fail (function(){
       alert("Error while communicating with Nimbus.");
   });
}

function confirmComponentAction(topologyName, componentId, componentName, action, param, defaultParamValue, paramText,
    actionText) {
  var opts = {
    type : 'POST',
    url : proxyPath() + 'api/v1/topology/debug?tname=' + topologyName + '&cid=' + componentId + '&debug=' + action
  };
  if (actionText === undefined) {
    actionText = action;
  }
  if (param) {
    var paramValue = prompt('Do you really want to ' + actionText + ' component "' + componentName + '"? '
        + 'If yes, please, specify ' + paramText + ':', defaultParamValue);
    if (paramValue != null && paramValue != "" && ensureInt(paramValue)) {
      opts.url += '&sampling-percentage=' + paramValue;
    } else {
      return false;
    }
  } else {
    if (typeof defaultParamValue !== 'undefined') {
      opts.url += '&sampling-percentage=' + defaultParamValue;
    }
    if (!confirm('Do you really want to ' + actionText + ' component "' + componentName + '"?')) {
      return false;
    }
  }

  $("input[type=button]").attr("disabled", "disabled");
  $.ajax(opts).always(function() {
    window.location.reload();
  }).fail(function() {
    alert("Error while communicating with Nimbus.");
  });

  return false;
}

function confirmAction(id, name, action, param, defaultParamValue, paramText, actionText) {
  var submitType = 'POST';
  var submitUrl = '';
  if (action == 'kill') {
    submitType = 'DELETE';
    submitUrl = proxyPath() + 'api/v1/topology?tname=' + name;
  } else if (action == 'rebalance') {
    submitType = 'PUT';
    submitUrl = proxyPath() + 'api/v1/topology?tname=' + name;
  } else if (action == 'deactivate') {
    submitType = 'PUT';
    submitUrl = proxyPath() + 'api/v1/topology/status?action=deactivate&tname=' + name;
  } else if (action == 'activate') {
    submitType = 'PUT';
    submitUrl = proxyPath() + 'api/v1/topology/status?action=activate&tname=' + name;
  } else if (action == 'rebalance') {
    submitType = 'PUT';
    submitUrl = proxyPath() + 'api/v1/topology?tname=' + name;
  } else if (action == 'debug/enable') {
    submitUrl = proxyPath() + 'api/v1/topology/debug?tname=' + name + '&debug=true';
  } else if (action == 'debug/disable') {
    submitUrl = proxyPath() + 'api/v1/topology/debug?tname=' + name + '&debug=false';
  }
  var opts = {
    type : submitType,
    url : submitUrl
  };
  if (param) {
    var waitSec = '5';
    if (action == 'kill') {
      waitSecs = prompt('Do you really want to ' + action + ' topology "' + name + '"? '
          + 'If yes, please, specify wait time in seconds:', defaultParamValue);
      if (waitSecs != null && waitSecs != "" && ensureInt(waitSecs)) {
        opts.url += '&wait-seconds=' + waitSecs;
      } else {
        return false;
      }
    } else if (action == 'rebalance') {
      waitSecs = prompt(
          'Do you really want to '
              + action
              + ' topology "'
              + name
              + '"? \n'
              + 'If yes, please, specify wait time/num workers/component->executors-num.\n'
              + 'For example, wait 5 seconds, provide 3 workers, word compoent provide 2 executor and updater compoent provide 3 executors.\n'
              + 'then fill in: 5/3/word:2,updater:3', '');
      if (waitSecs != null && waitSecs != '') {
        var params = waitSecs.split("/");
        if (params.length < 2) {
          alert("Error!!! wait time and workers' num is required");
          return false;
        }
        for (var i = 0; i < params.length; i++) {
          if (i == 0) {
            if (!ensureInt(params[i]) || params[i] < 1) {
              alert("Error!!! wait time must a positive number");
              return false;
            } else {
              opts.url += "&wait-seconds=" + params[i];
            }
          } else if (i == 1) {
            if (!ensureInt(params[i]) || params[i] < 1) {
              alert("Error!!! Workers' num must a positive number");
              return false;
            } else {
              opts.url += "&num-workers=" + params[i];
            }
          } else if (i == 2) {
            opts.url += "&executors=" + params[i];
          }
        }
      } else {
        return false;
      }
    } else if (action == 'debug/enable') {
      var paramValue = prompt('Do you really want to ' + actionText + ' topology "' + name + '"? '
          + 'If yes, please, specify ' + paramText + ':', defaultParamValue);
      if (paramValue != null && paramValue != "" && ensureInt(paramValue)) {
        opts.url += '&sampling-percentage=' + paramValue;
      } else {
        return false;
      }
    }
  } else if (!confirm('Do you really want to ' + action + ' topology "' + name + '"?')) {
    return false;
  }

  $("input[type=button]").attr("disabled", "disabled");
  $.ajax(opts).always(function() {
    if (action == 'kill') {
      window.location.href = 'index.html';
    } else {
      window.location.reload();
    }
  }).fail(function() {
    alert("Error while communicating with Nimbus.");
  });

  return false;
}

// $(function() {
// var placements = [ 'above', 'below', 'left', 'right' ];
// for ( var i in placements) {
// $('.tip.' + placements[i]).twipsy({
// live : true,
// placement : placements[i],
// delayIn : 1000
// });
// }
// });

function formatConfigData(data) {
  var mustacheFormattedData = {
    'config' : []
  };
  for ( var prop in data) {
    if (data.hasOwnProperty(prop)) {
      mustacheFormattedData['config'].push({
        'key' : prop,
        'value' : data[prop]
      });
    }
  }
  return mustacheFormattedData;
}

function formatErrorTimeSecs(response) {
  var errors = response["componentErrors"];
  for (var i = 0; i < errors.length; i++) {
    var time = errors[i]['time'];
    errors[i]['time'] = moment.utc(time).local().format("ddd, DD MMM YYYY HH:mm:ss Z");
  }
  return response;
}

function renderToggleSys(div) {
  var sys = $.cookies.get("sys") || false;
  if (sys) {
    div
        .append("<span data-original-title=\"Use this to toggle inclusion of storm system components.\" class=\"tip right\"><input onclick=\"toggleSys()\" value=\"Hide System Stats\" type=\"button\"></span>");
  } else {
    div
        .append("<span class=\"tip right\" title=\"Use this to toggle inclusion of storm system components.\"><input onclick=\"toggleSys()\" value=\"Show System Stats\" type=\"button\"></span>");
  }
}

function topologyActionJson(id, encodedId, name, status, msgTimeout, debug, samplingPct) {
  var jsonData = {};
  jsonData["id"] = id;
  jsonData["encodedId"] = encodedId;
  jsonData["name"] = name;
  jsonData["msgTimeout"] = msgTimeout;
  jsonData["activateStatus"] = (status === "ACTIVE") ? "disabled" : "enabled";
  jsonData["deactivateStatus"] = (status === "ACTIVE") ? "enabled" : "disabled";
  jsonData["rebalanceStatus"] = (status === "ACTIVE" || status === "INACTIVE") ? "enabled" : "disabled";
  jsonData["killStatus"] = (status !== "KILLED") ? "enabled" : "disabled";
  jsonData["startDebugStatus"] = (status === "ACTIVE" && !debug) ? "enabled" : "disabled";
  jsonData["stopDebugStatus"] = (status === "ACTIVE" && debug) ? "enabled" : "disabled";
  jsonData["currentSamplingPct"] = samplingPct;
  return jsonData;
}

function componentActionJson(topologyName, encodedId, componentName, status, debug, samplingPct) {
  var jsonData = {};
  jsonData["topologyName"] = topologyName;
  jsonData["encodedId"] = encodedId;
  jsonData["componentName"] = componentName;
  jsonData["startDebugStatus"] = (status === "ACTIVE" && !debug) ? "enabled" : "disabled";
  jsonData["stopDebugStatus"] = (status === "ACTIVE" && debug) ? "enabled" : "disabled";
  jsonData["currentSamplingPct"] = samplingPct;
  return jsonData;
}

function topologyActionButton(id, name, status, actionLabel, command, wait, defaultWait) {
  var buttonData = {};
  buttonData["buttonStatus"] = status;
  buttonData["actionLabel"] = actionLabel;
  buttonData["command"] = command;
  buttonData["isWait"] = wait;
  buttonData["defaultWait"] = defaultWait;
  return buttonData;
}

function go(name) {
  var new_path = location.pathname;
  var dir = '';
  if (null != new_path) {
    var reg = /(\/proxy\/application_[0-9]_[0-9]\/)|(\/port_[0-9]*\/)/;
    var regRet = reg.exec(new_path);
    if (null != regRet) {
      dir = regRet[0];
    } else {
      dir = "/";
    }
  }
  window.location.href = 'http://' + location.host + dir + name;
}

$.blockUI.defaults.css = {
  border : 'none',
  padding : '15px',
  backgroundColor : '#000',
  '-webkit-border-radius' : '10px',
  '-moz-border-radius' : '10px',
  'border-radius' : '10px',
  opacity : .5,
  color : '#fff',
  margin : 0,
  width : "30%",
  top : "40%",
  left : "35%",
  textAlign : "center"
};

/** /port_XXXX/ */
function proxyPath() {
  var new_path = location.pathname;
  var reslut = '';
  if (null != new_path) {
    var reg = /(\/proxy\/application_[0-9]_[0-9]\/)|(\/port_[0-9]*\/)/;
    var regRet = reg.exec(new_path);
    if (null != regRet) {
      reslut = regRet[0];
    }
  }
  return reslut;
}

/** return / or /port_XXX/ */
function getProxyPath(location) {
  var new_path = location;
  var reslut = '/';
  if (null != new_path) {
    var reg = /(\/proxy\/application_[0-9]_[0-9]\/)|(\/port_[0-9]*\/)/;
    var regRet = reg.exec(new_path);
    if (null != regRet) {
      reslut = regRet[0];
    }
  }
  return reslut;
}

if (!window.showModalDialog) {
  window.showModalDialog = function (arg1, arg2, arg3) {
     var w;
     var h;
     var resizable = "no";
     var scroll = "no";
     var status = "no";

     // get the modal specs
     var mdattrs = arg3.split(";");
     for (i = 0; i < mdattrs.length; i++) {
        var mdattr = mdattrs[i].split(":");

        var n = mdattr[0];
        var v = mdattr[1];
        if (n) { n = n.trim().toLowerCase(); }
        if (v) { v = v.trim().toLowerCase(); }

        if (n == "dialogheight") {
           h = v.replace("px", "");
        } else if (n == "dialogwidth") {
           w = v.replace("px", "");
        } else if (n == "resizable") {
           resizable = v;
        } else if (n == "scroll") {
           scroll = v;
        } else if (n == "status") {
           status = v;
        }
     }

     var left = window.screenX + (window.outerWidth / 2) - (w / 2);
     var top = window.screenY + (window.outerHeight / 2) - (h / 2);
     var targetWin = window.open(arg1, arg1, 'toolbar=no, location=no, directories=no, status=' + status + ', menubar=no, scrollbars=' + scroll + ', resizable=' + resizable + ', copyhistory=no, width=' + w + ', height=' + h + ', top=' + top + ', left=' + left);
     targetWin.focus();
  };
}