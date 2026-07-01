using Core.Settings;

namespace Core.Models.Requests;

public class MailRequest
{
    public MailRequestParams? Params { get; set; }
}

public class MailRequestParams
{
    public MailMsg? Msg { get; set; }
    public StmpSettings? Smtp { get; set; }
}
