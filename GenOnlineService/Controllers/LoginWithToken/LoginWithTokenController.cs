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

using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using MySqlX.XDevAPI.Common;
using System;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace GenOnlineService.Controllers.LoginWithToken
{

	public class POST_LoginWithToken_Result : APIResult
	{
		public override Type GetReturnType()
		{
			return typeof(POST_LoginWithToken_Result);
		}

		public EPendingLoginState result { get; set; } = EPendingLoginState.None;
		public string session_token { get; set; } = "";
		public string refresh_token { get; set; } = "";
		public Int64 user_id { get; set; } = -1;
		public string display_name { get; set; } = "";

		public string ws_uri { get; set; } = "";
	}

	[ApiController]
	[Authorize(Roles = "Player")]
	[Route("env/{environment}/contract/{contract_version}/[controller]")]
	public class LoginWithToken : ControllerBase
	{
		private readonly AppDbContext _db;

		public LoginWithToken(AppDbContext db)
		{
			_db = db;
		}

		[HttpPost(Name = "PostLoginWithToken")]
		//public async Task<APIResult> Post([FromHeader(Name = "CF-Connecting-IP")] string? ipAddress)
		public async Task<APIResult> Post()
		{
			using (var reader = new StreamReader(HttpContext.Request.Body))
			{
				string jsonData = await reader.ReadToEndAsync();

				bool bSecureWS = true;
				//if (ipCountry2LISO.ToLower() == "ru")
				{
					//bSecureWS = false;
				}

				POST_LoginWithToken_Result result = (POST_LoginWithToken_Result)await Post_InternalHandler(jsonData, IPHelpers.NormalizeIP(HttpContext.Connection.RemoteIpAddress?.ToString()), bSecureWS);
				return result;
			}
		}

		public async Task<APIResult> Post_InternalHandler(string jsonData, string ipAddr, bool bSecureWS, bool bWasMonitor = false)
		{
			if (bWasMonitor)
			{
				ipAddr = IPAddress.Loopback.ToString();
			}

			POST_LoginWithToken_Result result = new POST_LoginWithToken_Result();

			var options = new JsonSerializerOptions
			{
				PropertyNameCaseInsensitive = true
			};

			try
			{
				var data = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(jsonData, options);

				if (data != null && !data.ContainsKey("client_id"))
				{
					result.result = EPendingLoginState.LoginFailed;
					Response.StatusCode = (int)HttpStatusCode.Unauthorized;
				}
				else
				{
					if (data != null && data.ContainsKey("client_id"))
					{
						byte[] respNonce = new byte[32];
						using (RandomNumberGenerator rng = RandomNumberGenerator.Create()) { rng.GetBytes(respNonce); }

						// TODO_JWT: Look refresh token up in the revoked list
						// TODO_JWT: invalidate old refresh and session tokens

						// If you reach here, the refresh token was valid because auth happens globally
						string? clientID = data["client_id"].GetString();

						if (clientID != null && Program.g_tokenGenerator != null)
						{
							// start their session etc
							Int64 user_id = TokenHelper.GetUserID(this);

							string hwid_0 = data.ContainsKey("reserved_0") ? data["reserved_0"].ToString() : "NONE";
							string hwid_1 = data.ContainsKey("reserved_1") ? data["reserved_1"].ToString() : "NONE";
							string hwid_2 = data.ContainsKey("reserved_2") ? data["reserved_2"].ToString() : "NONE";
							await Database.Functions.Auth.RegisterUserDevice(GlobalDatabaseInstance.g_Database, user_id, hwid_0, hwid_1, hwid_2, ipAddr);

							// ban check
							bool bIsBanned = await Database.Users.IsUserBanned(_db, user_id);
							if (bIsBanned)
							{
								result.result = EPendingLoginState.LoginFailed;
								Response.StatusCode = (int)HttpStatusCode.Locked;
								return result;
							}

							string exe_crc = data.ContainsKey("exe_crc") ? data["exe_crc"].ToString() : "NONE";
							Helpers.RegisterInitialPlayerExeCRC(user_id, exe_crc);

							string strDisplayName = await Database.Functions.Auth.GetDisplayName(GlobalDatabaseInstance.g_Database, user_id);
							await Database.Functions.Auth.SetUsedLoggedIn(GlobalDatabaseInstance.g_Database, user_id, clientID);

							bool bIsAdmin = await Database.Users.IsUserAdmin(_db, user_id);

							result.result = EPendingLoginState.LoginSuccess;

							// extend token
							// TODO_TODAY_JWT: just get clientID from token
							var sessiontoken = Program.g_tokenGenerator.GenerateToken(strDisplayName, user_id, ipAddr, Program.JwtTokenGenerator.ETokenType.Session, clientID, bIsAdmin);
							var refreshtoken = Program.g_tokenGenerator.GenerateToken(strDisplayName, user_id, ipAddr, Program.JwtTokenGenerator.ETokenType.Refresh, clientID, false);
							result.session_token = sessiontoken;
							result.refresh_token = refreshtoken;

							result.user_id = user_id;
							result.display_name = strDisplayName;

							result.ws_uri = Program.GetWebSocketAddress(bSecureWS);

							// clear cached data, its a refresh websocket connection
							WebSocketManager.ClearDataFromUser(user_id);
						}
						else
						{
							result.result = EPendingLoginState.LoginFailed;
							Response.StatusCode = (int)HttpStatusCode.Unauthorized;
							return result;
						}
					}
					else
					{
						// TODO: Log this
						//sess.SendResponseAsync(sess.Response.MakeGetResponse("Missing Key"));

						return result;
					}
				}
			}
			catch
			{
				return result;
			}

			return result;
		}
	}
}
