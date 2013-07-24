(function() {
  jQuery(function() {
    return $("button").click(function() {
      var _this = this;

      return $.ajax({
        type: "GET",
        url: "api/dataservice/" + $(this).data("table") + "?type=ALL",
        success: function(data) {
          var $notice;

          $notice = $("<div id='notice' class='alert alert-info hide'>" + "<a class='close' data-dismiss='alert' href='#'>&times;</a>" + "<div id='message'></div></div>");
          $("#notices").append($notice);
          return $notice.fadeIn('slow').addClass('alert-info').children('div').html(data);
        },
        beforeSend: function() {
          $(_this).removeClass('btn-primary').addClass('btn-warning');
          return $(_this).button('loading');
        },
        timeout: function() {
          return $("#notice").fadeIn('slow').addClass('alert-warning').text("服务器没有响应，请稍后再试，或者联系管理员！");
        },
        error: function(message) {
          return $("#notice").fadeIn('slow').addClass('alert-error').children('div').html("<p>执行过程中发生了错误，请尝试联系管理员解决! </br>错误信息：" + message + "</p>");
        },
        complete: function() {
          return $(_this).removeClass('btn-warning').addClass('btn-primary').button('reset');
        }
      });
    });
  });

}).call(this);
