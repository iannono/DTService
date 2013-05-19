jQuery ->
  $("button").click -> 
    $.ajax(
      type: "GET",
      url: "api/dataservice/" + $(this).data("table"),
      success: (data) =>
        $notice = $("<div id='notice' class='alert alert-info hide'>" +
                    "<a class='close' data-dismiss='alert' href='#'>&times;</a>" +
                    "<div id='message'></div></div>")
        $("#notices").append($notice)
        $notice.fadeIn('slow').addClass('alert-info').children('div').html(data)

      beforeSend: =>
        $(this).removeClass('btn-primary').addClass('btn-warning')
        $(this).button('loading')
        
      timeout: ->
        $("#notice").fadeIn('slow').addClass('alert-warning').text("服务器没有响应，请稍后再试，或者联系管理员！")

      error: (message) ->
        $("#notice").fadeIn('slow').addClass('alert-error').children('div').html("<p>执行过程中发生了错误，请尝试联系管理员解决! </br>错误信息：" + message + "</p>")

      complete: =>
        $(this).removeClass('btn-warning').addClass('btn-primary').button('reset') 
    )
      