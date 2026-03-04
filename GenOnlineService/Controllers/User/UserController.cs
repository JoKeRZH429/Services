/*
**    GeneralsOnline Game Services - Backend Services for Command & Conquer Generals Online: Zero Hour
**    Copyright (C) 2025  GeneralsOnline Development Team
**
**    This program is free software: you can redistribute it and/or modify
**    it under the terms of the GNU Affero General Public License as
**    published by the Free Software Foundation, either version 3 of the
**    License, or (at your option) any later version.
**
**    This program is distributed in the hope that it will be useful,
**    but WITHOUT ANY WARRANTY; without even the implied warranty of
**    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
**    GNU Affero General Public License for more details.
**
**    You should have received a copy of the GNU Affero General Public License
**    along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

using Amazon.S3.Model;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Net.WebSockets;
using System.Security.Claims;
using System.Text;
using System.Text.Json;

namespace GenOnlineService.Controllers
{
	public class RouteHandler_DELETE_User_Result : APIResult
	{
		public override Type GetReturnType()
		{
			return this.GetType();
		}

		public bool success { get; set; } = false;
	}

	[ApiController]
	[Authorize(Roles = "Player")]
	[Route("env/{environment}/contract/{contract_version}/[controller]")]
	public class UsersController : ControllerBase
	{
		private readonly AppDbContext _db;
		private readonly ILogger<UsersController> _logger;

		public UsersController(AppDbContext db, ILogger<UsersController> logger)
		{
			_logger = logger;
			_db = db;
		}

		[Authorize(Roles = "Player")]
		[HttpGet("Me")]
		public async Task<APIResult> MyUser()
		{
			GET_MyUser_Result result = new GET_MyUser_Result();

			Int64 user_id = TokenHelper.GetUserID(this);

			if (user_id != -1)
			{
				string strDisplayName = await Database.Users.GetDisplayName(_db, user_id);

				result.display_name = strDisplayName;
				result.user_id = user_id;
			}

			return result;
		}

		[Authorize(Roles = "Player")]
		[HttpGet("Active")]
		public APIResult ActiveUsers()
		{
			GET_ActiveUsers_Result result = new GET_ActiveUsers_Result();

			string TimeSpanToHumanReadableString(TimeSpan timeSpan)
			{
				string humanReadable = $"{(timeSpan.Days > 0 ? $"{timeSpan.Days} days, " : "")}" +
					   $"{(timeSpan.Hours > 0 ? $"{timeSpan.Hours} hours, " : "")}" +
					   $"{(timeSpan.Minutes > 0 ? $"{timeSpan.Minutes} minutes, " : "")}" +
					   $"{timeSpan.Seconds} seconds";
				humanReadable = humanReadable.TrimEnd(',', ' ');
				return humanReadable;
			}

			// TODO_QUICKMATCH: We chekc maps are big enough, but the reverse needs checked too - dont let 8 playrs join a 6-8 ffa if only map is defcon6 for example

			var allData = WebSocketManager.GetUserDataCache();
			foreach (var sessionData in allData)
			{
				GET_ActiveUsers_UserEntry userEntry = new();
				userEntry.name = sessionData.Value.m_strDisplayName;
				userEntry.status = UserPresence.DetermineUserStatus(sessionData.Value);
				userEntry.client_id = sessionData.Value.m_client_id;
				userEntry.duration = TimeSpanToHumanReadableString(sessionData.Value.GetDuration());

				result.active_users.Add(userEntry);
			}


			return result;
		}
	}

	[ApiController]
	[Authorize(Roles = "Player")]
	[Route("env/{environment}/contract/{contract_version}/[controller]")]
	public class UserController : ControllerBase
	{
		private readonly ILogger<UserController> _logger;

		public UserController(ILogger<UserController> logger)
		{
			_logger = logger;
		}

		[HttpDelete]
		public async Task<APIResult> Delete()
		{
			RouteHandler_DELETE_User_Result result = new RouteHandler_DELETE_User_Result();

			Int64 user_id = TokenHelper.GetUserID(this);

			if (user_id != -1)
			{
				// TODO_JWT: Add token used to a 'ban list'
				//string token = "";

				// end session
				UserSession? session = WebSocketManager.GetDataFromUser(user_id);
				if (session != null)
				{
					UserWebSocketInstance ws = await session.CloseWebsocket(WebSocketCloseStatus.NormalClosure, "User logged out");
					await WebSocketManager.DeleteSession(user_id, ws, true);
				}
			}

			return result;
		}
	}
}
