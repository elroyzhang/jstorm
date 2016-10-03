$(document).ready(function() {
  loadJarList();
});
function loadJarList(proxy_path) {
  var url = proxyPath() + "api/v1/topology/jar?jar-type=topology.jar";
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
      tr.append("<td><input type='checkbox' id='jarCheck'></input></td>");
      tr.append("<td id='tdFileName'>" + fileName + "</td>");
      tr.append("<td>" + uploadTime + "</td>");
      tbody.append(tr);
    }
  }
  $('#tableUpLoaded tbody').replaceWith(tbody);
  $("#tableUpLoaded > tbody tr").each(function() {
    $(this).find("#jarCheck").bind("click", function() {
      var $td = $(this).parents('td').parents('tr').children('td');
      showSubmitDiv($(this).prop("checked"), $td.eq(1).text());
    });
  });
}

function showSubmitDiv(checked, fileName) {
  clearError();
  var submitDiv = $("#submitDiv");
  var addJarDiv = $("#addJarDiv");
  if (checked == true) {
    $("#tableUpLoaded > tbody tr").each(function() {
      var tmpFileName = $(this).find("#tdFileName").text();
      if (fileName != tmpFileName) {
        $(this).find("#jarCheck").prop("checked", false);
      }
    });
    submitDiv.css("display", "");
    addJarDiv.css("display", "none");
  } else {
    var allNoChoose = true;
    $("#tableUpLoaded > tbody tr").each(function() {
      if ($(this).find("#jarCheck").prop("checked")) {
        allNoChoose = false;
      }
    });
    if (allNoChoose) {
      submitDiv.css("display", "none");
      addJarDiv.css("display", "");
    }
  }
}

function removeAllCheck() {
  $("#tableUpLoaded > tbody tr").each(function() {
    $(this).find("#jarCheck").removeAttr("checked");
  });
}
function getChooseJarName() {
  var jarName = "";
  $("#tableUpLoaded > tbody tr").each(function() {
    if ($(this).find("#jarCheck").prop("checked")) {
      jarName = $(this).find("#tdFileName").text();
    }
  });
  return jarName;
}

function submitTopology() {
  clearError();
  var jarName = getChooseJarName();
  if (jarName == null || jarName == "") {
    alert("Please choose a jar!");
    return;
  }
  var mainClass = $("#submitDiv").find("#mainClass").val();
  if (mainClass == null || mainClass == "") {
    alert("main class can not be empty!!");
    return;
  }
  var args = assembleParams();
  var stormOptions = assembleStormOptions();
  var submitUrl = proxyPath() + "api/v1/topology?jar-type=topology.jar&jar-name=" + jarName + "&main-class=" + mainClass + "&args="
      + args + "&storm-options=" + stormOptions;
  var btnSubmit = $("#btnSubmit");
  btnSubmit.attr("disabled", "disabled");

  $.ajax({
    url : submitUrl,
    dataType : "json",
    async : true,
    type : "POST",
    success : function(data) {
      var resultCode = data["result_code"];
      if (resultCode != "200") {
        $("#error").text(data["result_msg"]);
        btnSubmit.removeAttr("disabled");
        return false;
      } else {
        alert("submit success!");
        btnSubmit.removeAttr("disabled");
        removeAllCheck();
        clearSubmitValue();
        window.location.reload();
      }
    },
  });
}
function clearSubmitValue() {
  $("#submitDiv").find("input[type='text']").each(function () {
    $(this).val("");
  });
}
function clearError() {
  $("#error").text("");
}
function upLoadJar() {
  var formData = new FormData();
  if ($('#topologyJar').val() == null || $('#topologyJar').val() == "") {
    alert("Please choose jar file first!")
    return;
  }
  var file = $('#topologyJar')[0].files[0];
  var jarName = file.name;
  if (!jarName.endsWith(".jar")) {
    alert("only jar file can be upload!")
    return;
  }
  formData.append('file', file);
  $.ajax({
    url : proxyPath() + 'api/v1/topology/jar?jar-type=topology.jar&jar-name=' + jarName,
    type : 'POST',
    dataType : "json",
    cache : false,
    data : formData,
    processData : false,
    contentType : false
  }).done(function(res) {
    if (res["result_code"] == "200") {
      alert("upload success!");
      $('#btnUpload').attr("disabled", "disabled");
      $('#jarName').val("");
      $('#topologyJar').val("");
      window.location.reload();
    } else {
      $("#error").text(res["result_msg"]);
    }
  });
}
function addJar() {
  $('#topologyJar').click();
}
function jarSelcted() {
  $('#jarName').val($('#topologyJar')[0].files[0].name);
  $('#btnUpload').val("upload");
  $('#btnUpload').removeAttr("disabled");
}

function deleteJar() {
  var jarName = getChooseJarName();
  if (jarName == null || jarName == "") {
    alert("Please choose a jar!");
    return;
  }
  if (!confirm('Do you really want to delete file : ' + jarName + " ?")) {
    return;
  }
  $.ajax({
    url : proxyPath() + 'api/v1/topology/jar?jar-type=topology.jar&jar-name=' + jarName,
    type : 'DELETE',
    dataType : "json",
    cache : false,
    processData : false,
    contentType : false
  }).done(function(res) {
    if (res["result_code"] == "200") {
      alert(res["result_msg"]);
      $('#btnUpload').attr("disabled", "disabled");
      $('#jarName').val("");
      $('#topologyJar').val("");
      window.location.reload();
    } else {
      $("#error").text(res["result_msg"]);
    }
  });
}
function addParam() {
  var submitDivTbody = $("#submitDiv").find("tbody").find("#argsTrRoot");
  var topologyArgsTr = '<tr id="argsTr"><td style="width:20%"><label>topology args:</label></td>'
      + '<td style="width:20%"><input type="text" id="args" class="args"></input>'
      + ' <input type="button" value="-" id="btnDelParam" onClick="delParam(this)" /></td>'
      + '<td style="width: 20%"><input type="file" name="topologyArgsFile" id="topologyArgsFile" onchange="argsFileSet(this)" /></td>'
      + '<td style="width: 40%"></td></tr>';
  submitDivTbody.after(topologyArgsTr);
}
function addStormOptionsParam() {
  var submitDivTbody = $("#submitDiv").find("tbody").find("#stormOptionsTrRoot");
  var topologyArgsTr = '<tr id="stormOptionsTr"><td style="width:20%"><label>storm options:</label></td>'
      + '<td style="width:20%"><input type="text" id="stormOptions" class="stormOptions"></input>' 
      + ' <input type="button" value="-" id="btnDelParam" onClick="delParam(this)" /></td>'
      + '<td style="width: 20%"></td>'
      + '<td style="width: 40%"></td></tr>';
  submitDivTbody.after(topologyArgsTr);
}


function delParam(btn) {
  $(btn).parent().parent().remove();
}
function argsFileSet(argsFileInput) {
  var paramFile = $(argsFileInput)[0].files[0];
  var paramFileName = paramFile.name;
  if (!paramFileName.endsWith(".jar") && !paramFileName.endsWith(".xml") && !paramFileName.endsWith(".yaml")
      && !paramFileName.endsWith(".properties")) {
    alert("only jar/xml/yaml/properties file can be upload!")
    return;
  }
  var jarName = getChooseJarName();
  var formData = new FormData();
  formData.append('file', paramFile);
  $.ajax({
    url : proxyPath() + 'api/v1/topology/jar?jar-type=topology.jar&jar-name=' + jarName + "&param-file-name=" + paramFileName,
    type : 'POST',
    dataType : "json",
    cache : false,
    data : formData,
    processData : false,
    contentType : false
  }).done(function(res) {
    if (res["result_code"] == "200") {
      $(argsFileInput).parent().parent().find("#args").val(paramFileName);
      //$(argsFileInput).parent().parent().find("#args").attr("disabled", "disabled");
    } else {
      alerty(res["result_msg"]);
      $(argsFileInput).parent().parent().find("#args").val("");
      //$(argsFileInput).parent().parent().find("#args").attr("disabled", "");
    }
  });
}
function assembleParams() {
  var argsStr = "";
  $("#submitDiv").find("table").find("tbody").find("tr").each(function() {
    if ($(this).attr("id") == "argsTrRoot" || $(this).attr("id") == "argsTr") {
      argsStr += $(this).find(":input[id=args]").val() + ",";
    }
  });
  return argsStr;
}

function assembleStormOptions() {
  var argsStr = "";
  $("#submitDiv").find("table").find("tbody").find("tr").each(function() {
    if ($(this).attr("id") == "stormOptionsTrRoot" || $(this).attr("id") == "stormOptionsTr") {
      var tmpOptions = $(this).find(":input[id=stormOptions]").val();
      if (tmpOptions != null && tmpOptions != "") {
        argsStr += tmpOptions.trim() + ",";
      }
    }
  });
  return argsStr;
}