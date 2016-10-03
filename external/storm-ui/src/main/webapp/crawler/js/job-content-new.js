var siteId;
function loadJobConfig() {
  var proxy_path = proxyPath();
  var url = proxy_path + "api/v1/crawler/conf?site.id=" + siteId;
  $.ajaxSetup({
    "error" : function(jqXHR, textStatus, response) {
      var errorJson = jQuery.parseJSON(jqXHR.responseText);
      $.get(proxy_path + "templates/json-error-template.html",
          function(template) {
            $("#json-response-error").append(
                Mustache.render($(template).filter("#json-error-template").html(), errorJson));
          });
    }
  });
  $.getJSON(url, function(response, status, jqXHR) {
    var jobContent = $("#job-content");
    if (response["result_code"] == 500) {// exception
      $.get(proxy_path + "templates/response-error-template.html", function(template) {
        $("#response-internal-server-error").append(
            Mustache.render($(template).filter("#response-error-template").html(), response));
      });
    } else {
      jobContent.append(response);
//      var jobConfs = response['job-confs'];
//      if (jobConfs.length < 1) {
//        return;
//      }
//      var jobConf = jobConfs[0];
//      for ( var tmp in jobConf) {
//        var key = tmp;
//        if ("site.id" == key) {
//          continue;
//        }
//        var content = jobConf[key];
//        jobContent.append(key).append(": ");
//        if (typeof (content) == "Array" || typeof (content) == "object") {
//          for (var i = 0; i < content.length; i++) {
//            jobContent.append("\n");
//            jobContent.append("  ").append(content[i]);
//          }
//        } else {
//          jobContent.append("").append('' + content);
//        }
//        jobContent.append("\n");
//      }
    }
  });
}

function initJobContentTemplate() {
  var jobContent = $("#job-content");
  jobContent.append("site.id").append(": n").append("\n");
  jobContent.append("site.type").append(": 2").append("\n");
  jobContent.append("crawl.job.extractor.class:").append("\n").append(" - com.tencent.tdspider.rules.qq.QQComRuleB").append("\n");
  jobContent.append("crawl.job.childopts").append(": -X1024m").append("\n");
  jobContent.append("filter.regex.list").append(": ").append("\n");
  jobContent.append(" - .*(\.(css|js|gif|jpg|png|mp3|mp3|zip|gz))$").append("\n");
  jobContent.append(" - .*coral.qq.com.*").append("\n");
  jobContent.append("match.regex.list").append(": ").append("\n");
  jobContent.append(" - .*.qq.com/a/\d+/\d+\.htm.*").append("\n");
  jobContent.append(" - .*.qq.com/origna/\d+/\d+\.htm.*").append("\n");
  jobContent.append("seeds").append(": ").append("\n");
  jobContent.append(" - http://news.qq.com/").append("\n");
  jobContent.append(" - http://tech.qq.com/").append("\n");
  jobContent.append("number.of.crawlers").append(": ").append("\n");
  jobContent.append("crawl.job.schedule.frequence").append(": ").append("\n");
  jobContent.append("cookies.ignore").append(": ").append("\n");
  jobContent.append("cookies.load.file").append(": ").append("\n");
  jobContent.append("cookies.domain").append(": ").append("\n");
  jobContent.append("politeness.delay.max").append(": ").append("\n");
  jobContent.append("politeness.delay.min").append(": ").append("\n");
  jobContent.append("robots.is.obey").append(": ").append("\n");
  jobContent.append("tdspider.debug").append(": ").append("\n");
}

function replaceWord(val) {
  val = val.replace(/\+/g, "%2B");
  // val = val.replace(/\//g, "%2F");
  val = val.replace(/\?/g, "%3F");
  val = val.replace(/\#/g, "%23");
  val = val.replace(/\&/g, "%26");
  val = val.replace(/\=/g, "%3D");
  return val;
}
function getValues(arr, index) {
  var retVal = "";
  for (var i = index; i < arr.length; i++) {
    var tmp = arr[i];
    var valArr = tmp.split(":");
    if (i == index) {
      if (valArr.length > 1) {
        retVal = valArr[1];
        retVal = retVal.trim();
        if (retVal != "") {
          retVal = replaceWord(retVal);
          break;
        }
      }
    } else {
      if (tmp.indexOf(":") > -1 && tmp.trim().indexOf("http:") == -1 && tmp.trim().indexOf("https:") == -1) {
        break;
      } else {
        tmp = tmp.trim();
        tmp = replaceWord(tmp);
        retVal += tmp + ",";
      }
    }
  }
  return retVal;
}
function closeJobConf() {
  window.close();
}

function saveJobConf() {
  $("input[type=button]").attr("disabled", "disabled");
  var jobContent = $("#job-content").val();
  var jobContentArr = jobContent.split(/\r?\n/);
  var paramData = {};
  for (var i = 0; i < jobContentArr.length; i++) {
    var tmp = jobContentArr[i];
    if (tmp != null && tmp.trim().startsWith("http:")) {
      continue;
    }
    var keyIndex = tmp.indexOf(":");
    if (keyIndex > -1) {
      var key = tmp.substr(0, keyIndex);
      var value = getValues(jobContentArr, i);
      // submitUrl += "&" + key + "=" + value;
      paramData[key] = value;
    }
  }
  if (!validPropertites(paramData)) {
    $("input[type=button]").removeAttr("disabled");
    return;
  }
  var submitSiteId = paramData["site.id"];
  var submitType = "PUT";
  if (action == "create") {
    submitType = "POST";
  } else {
    if (submitSiteId != siteId) {
      alert("site.id can not change!!!");
      $("input[type=button]").removeAttr("disabled");
      return;
    }
  }
  var fileData = {};
  fileData["jobFileString"] = jobContent;
  var submitUrl = proxyPath() + "api/v1/crawler/conf?site.id=" + submitSiteId;
  $.ajax({
    url : submitUrl,
    dataType : "json",
    data : fileData,
    async : true,
    type : submitType,
    success : function(data) {
      var resultCode = data["result_code"];
      if (resultCode != "200") {
        alert(data["result_msg"]);
        $("input[type=button]").removeAttr("disabled");
        return false;
      } else {
        alert(data["result_msg"]);
        siteId = data["result_content"]["site.id"];
        $("input[type=button]").removeAttr("disabled");
        setJobName(siteId);
        setJobConfigVal(siteId);
        $('#divJobContent').toggle();
        window.returnValue = "true"
      }
    }
  });
}
function validPropertites(paramData) {
  var prop = paramData["site.id"];
  if (prop == null || prop == "") {
    alert("site.id can not be null!!!")
    return false;
  } else {
    var isInt = /^\d+$/.test(prop);
    if (!isInt) {
      alert("site.id must be an int value!");
      return false;
    }
  }
  var prop = paramData["site.type"]
  if (prop == null || prop == "") {
    alert("site.type can not be null!!!")
    return false;
  }
  prop = paramData["crawl.job.extractor.class"]
  if (prop == null || prop == "") {
    alert("crawl.job.extractor.class can not be null!!!")
    return false;
  }
  prop = paramData["filter.regex.list"]
  if (prop == null || prop == "") {
    alert("filter.regex.list can not be null!!!")
    return false;
  }
  prop = paramData["match.regex.list"]
  if (prop == null || prop == "") {
    alert("match.regex.list can not be null!!!")
    return false;
  }
  prop = paramData["seeds"]
  if (prop == null || prop == "") {
    alert("seeds can not be null!!!")
    return false;
  }
  return true;
}

function clearError() {
  $("#error").text("");
}
function addJar() {
  $('#topologyJar').click();
}
function jarSelcted() {
  if (siteId == "") {
    alert("Config job file first");
    return;
  }
  $('#jarName').val($('#topologyJar')[0].files[0].name);
  $('#btnUpload').val("upload");
  $('#btnUpload').removeAttr("disabled");
}
function getChooseJarName() {
  var jarName = "";
  $("#tableUpLoaded > tbody tr").each(function() {
    if ($(this).find("#tdFileName").text() != null && $(this).find("#tdFileName").text() != "") {
      jarName = $(this).find("#tdFileName").text();
    }
  });
  return jarName;
}
function upLoadJar() {
  $('#btnUpload').attr("disabled", "disabled");
  var formData = new FormData();
  if ($('#topologyJar').val() == null || $('#topologyJar').val() == "") {
    alert("Please select jar file first!")
    $('#btnUpload').remove("disabled");
    return;
  }
  var file = $('#topologyJar')[0].files[0];
  var jarName = file.name;
  if (!jarName.endsWith(".jar")) {
    alert("only jar file can be upload!");
    $('#btnUpload').remove("disabled");
    return;
  }
  formData.append('file', file);
  $.ajax({
    url : proxyPath() + 'api/v1/topology/jar?jar-type=tdspider.jar&jar-name=' + jarName + "&site.id=" + siteId,
    type : 'POST',
    dataType : "json",
    cache : false,
    data : formData,
    processData : false,
    contentType : false
  }).done(function(res) {
    if (res["result_code"] == "200") {
      alert("upload success!");
      $('#btnUpload').remove("disabled");
      $('#jarName').val("");
      $('#topologyJar').val("");
      loadJarList();
      $('#addJarDiv').toggle();
    } else {
      alert(res["result_msg"]);
      $('#btnUpload').remove("disabled");
    }
  });
}
function loadJarList() {
  var url = proxyPath() + "api/v1/topology/jar?jar-type=tdspider.jar&site.id=" + siteId;
  $.getJSON(url, function(response, status, jqXHR) {
    var resultCode = response["result_code"];
    if (resultCode != "200") {
      var jarList = response["jar-files"];
      renderJarList(jarList);
    }
  });
}
function renderJarList(jarList) {
  var tableUploaded = $("#tableUpLoaded");
  var tbody = $('<tbody></tbody>');
  if (jarList != null && jarList.length > 0) {
    for (var i = 0; i < jarList.length; i++) {
      var tr = $('<tr></tr>');
      var jarFile = jarList[i];
      var fileName = jarFile["file-name"];
      var uploadTime = jarFile["upload-time"];
      tr.append("<td id='tdFileName'>" + fileName + "</td>");
      tr.append("<td>" + uploadTime + "</td>");
      tbody.append(tr);
    }
  }
  $('#tableUpLoaded tbody').replaceWith(tbody);
}
function submitTopology() {
  if (siteId == "") {
    alert("Config job file first");
    return;
  }
  var jarName = getChooseJarName();
//  if (jarName == null || jarName == "") {
//    alert("Please choose a jar!");
//    return;
//  }
  var submitUrl = proxyPath() + "api/v1/crawler/jobs?jar-type=tdspider.jar&jar-name=" + jarName + "&site.id=" + siteId;
  var btnSubmit = $("#btnSubmit");
  btnSubmit.prop("disabled", "disabled");
  $.ajax({
    url : submitUrl,
    dataType : "json",
    async : true,
    type : "POST",
    success : function(data) {
      var resultCode = data["result_code"];
      if (resultCode != "200") {
        alert(data["result_msg"]);
        btnSubmit.removeAttr("disabled");
        return false;
      } else {
        alert("submit success!");
        btnSubmit.removeAttr("disabled");
        window.returnValue = true;
        window.close();
      }
    },
  });
}
