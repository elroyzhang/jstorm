function logFileSelect() {
  var div = $("#divLogFileSelect");
  var file = $("#selFile").val();
  var url = div.attr("name") + "?file=" + file;
  window.location.href = url;
}

function searchFile() {
  var search = $("#divSearch").find("#search").val();
  var isDaemon = $("#divSearch").find("#is-daemon").val();
  var file = $("#divSearch").find("#file").val();
  var url = 'logviewer_search.html?file=' + file + '&search=' + search + '&is-daemon=' + isDaemon;
  window.open(url);
}
