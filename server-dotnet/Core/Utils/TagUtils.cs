using Core.Models;
using System;
using System.Collections.Generic;
using System.Text;

namespace Core.Utils;

public static class TagUtils
{

    public static void TagValueCompose(object? value, object? newValue, Tag tag)
    {
        if (!(tag.Type == "String" || tag.Type == "ByteString" || tag.Type == "string"))
        {

            if (tag.Scale != null)
            {
      
            }


        }
    }

}

