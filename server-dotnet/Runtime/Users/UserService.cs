using Core.Entity;
using Core.Models;
using Core.Response;
using Core.Settings;
using Core.Utils;
using Microsoft.AspNetCore.Identity.Data;
using Microsoft.Extensions.Logging;
using Runtime.Project;
using Runtime.Storage;
using SqlSugar;
using System;
using System.Collections.Generic;
using System.IdentityModel.Tokens.Jwt;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using LoginRequest = Core.Models.LoginRequest;

namespace Runtime.Users;

public class UserService
{
    private readonly ILogger<UserService> _logger;
    private readonly ISqlSugarClient _db;
    private static JsonSerializerOptions _jsonOptions = new JsonSerializerOptions
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };
    public UserService(ILogger<UserService> logger, ISqlSugarProvider provider)
    {
        _logger = logger;
        _db = provider.GetClient("UserService");
        try
        {
            _db.CodeFirst.InitTables<User>();
            _db.CodeFirst.InitTables<Role>();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "初始化表失败");
        }
    }


    public async Task SetDefault()
    {
        var user = new User
        {
            Username = "admin",
            FullName = "Administrator Account",
            Password = BCrypt.Net.BCrypt.HashPassword("123456"),
            Groups = -1
        };
        await _db.Insertable(user).ExecuteCommandAsync();
    }

    public Task<List<User>> GetUsers(string? username)
    {
        return _db.Queryable<User>().WhereIF(!string.IsNullOrEmpty(username), x => x.Username == username).ToListAsync();
    }

    public async Task SetUser(User user)
    {
        var _user = await _db.Queryable<User>().Where(x => x.Username == user.Username).FirstAsync();
        var hasPwd = !string.IsNullOrEmpty(_user.Password);
        //账号已存在且原密码不为空，则需要判断是否修改了密码，如果修改了则进行加密，否则保持原密码不变
        //1.账号已存在且密码为空，则保持原密码不变（不更新密码字段）
        if (_user != null && !hasPwd)
        {
            await _db.Updateable(user).IgnoreColumns(x => x.Password).ExecuteCommandAsync();
        }
        //2.账号已存在且密码不为空，对密码进行加密
        else if (_user != null && hasPwd)
        {
            user.Password = BCrypt.Net.BCrypt.HashPassword(user.Password);
            await _db.Updateable(user).ExecuteCommandAsync();
        }
        //3.账号不存在且密码不为空，若没有密码则使用默认密码123456并进行加密
        else
        {
            user.Password = hasPwd ? user.Password : "123456";
            user.Password = BCrypt.Net.BCrypt.HashPassword(user.Password);
            await _db.Insertable(user).ExecuteCommandAsync();
        }
    }

    public async Task RemoveUser(string username)
    {
        await _db.Deleteable<User>().Where(x => x.Username == username).ExecuteCommandAsync();
    }

    public async Task<List<Role>> GetRoles()
    {
        return await _db.Queryable<Role>().ToListAsync();
    }

    public async Task SetRole(Role role)
    {
        var existing = await _db.Queryable<Role>().Where(x => x.Name == role.Name).FirstAsync();
        if (existing != null)
        {
            await _db.Updateable(role).ExecuteCommandAsync();
        }
        else
        {
            await _db.Insertable(role).ExecuteCommandAsync();
        }
    }

    public async Task RemoveRoles(List<string> roleNames)
    {
        await _db.Deleteable<Role>().Where(x => roleNames.Contains(x.Name)).ExecuteCommandAsync();
    }

    public async Task<ResponseBase<LoginResponse>> Singin(LoginRequest req)
    {
        var response = new ResponseBase<LoginResponse>();
        var user = await _db.Queryable<User>().FirstAsync(x => x.Username == req.Username);
        if (user == null)
        {
            response.Status = "error";
            response.Message = "账号或密码错误";
            return response;
        }
        bool valid = BCrypt.Net.BCrypt.Verify(req.Password, user.Password);
        if (!valid)
        {
            response.Status = "error";
            response.Message = "账号或密码错误";
            return response;
        }
        response.Status = "success";
        response.Message = "user found!!!";
        response.Data = new LoginResponse
        {
            Username = user.Username,
            Fullname = user.FullName,
            Groups = user.Groups,
            Token = user.ToJwtToken()
        };
        return response;
    }
}
