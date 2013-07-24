jQuery ->
  $("button").click -> 
    $.ajax(
      type: "GET",
      url: "api/dataservice/" + $(this).data("table") + "?type=ALL",
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
        $("#notice").fadeIn('slow').addClass('alert-warning').text("������û����Ӧ�����Ժ����ԣ�������ϵ����Ա��")

      error: (message) ->
        $("#notice").fadeIn('slow').addClass('alert-error').children('div').html("<p>ִ�й����з����˴����볢����ϵ����Ա���! </br>������Ϣ��" + message + "</p>")

      complete: =>
        $(this).removeClass('btn-warning').addClass('btn-primary').button('reset') 
    )
      