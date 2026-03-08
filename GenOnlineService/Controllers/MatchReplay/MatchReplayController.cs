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
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Infrastructure;
using Microsoft.Extensions.Options;
using System;
using System.Net;
using System.Net.WebSockets;
using System.Security.Claims;
using System.Text;
using System.Text.Json;
using Amazon.S3;
using Amazon.S3.Model;
using System.ComponentModel.DataAnnotations;
using Org.BouncyCastle.Tls;

namespace GenOnlineService.Controllers
{
	public class RouteHandler_PUT_MatchReplay_Result : APIResult
	{
		public override Type GetReturnType()
		{
			return this.GetType();
		}
	}

	[ApiController]
	[Route("env/{environment}/contract/{contract_version}/[controller]")]
	public class MatchReplayController : ControllerBase
	{
		private readonly ILogger<LobbiesController> _logger;
		private readonly LobbyManager _lobbyManager;

		public MatchReplayController(LobbyManager lobbyManager, ILogger<LobbiesController> logger)
		{
			_logger = logger;
			_lobbyManager = lobbyManager;
		}

		[HttpPut]
		[RequestSizeLimit(2097152)] // 2MB
		[Authorize(Roles = "GameClient")]
		public async Task<APIResult> Post()
		{
			RouteHandler_POST_Lobby_Result result = new RouteHandler_POST_Lobby_Result();

			if (Program.g_Config == null)
			{
				Response.StatusCode = (int)HttpStatusCode.NotImplemented;
				return result;
			}

			IConfiguration? matchdataSettings = Program.g_Config.GetSection("MatchData");

			if (matchdataSettings == null)
			{
				Response.StatusCode = (int)HttpStatusCode.NotImplemented;
				return result;
			}

			// is upload enabled?
			bool bUploadMatchData = matchdataSettings.GetValue<bool>("upload_match_data");
			if (!bUploadMatchData)
			{
				Response.StatusCode = (int)HttpStatusCode.NotImplemented;
				return result;
			}

			string? strS3AccessKey = matchdataSettings.GetValue<string>("s3_access_key");
			string? strS3SecretKey = matchdataSettings.GetValue<string>("s3_secret_key");
			string? strS3BucketName = matchdataSettings.GetValue<string>("s3_bucket_name");
			string? strS3Endpoint = matchdataSettings.GetValue<string>("s3_endpoint");

			// must be in a lobby
			Int64 user_id = TokenHelper.GetUserID(this);
			EUserSessionType sessionType = TokenHelper.GetSessionType(this);
			if (user_id != -1 && SessionHelpers.SessionTypeHasAccessTo(sessionType, ESessionAccessType.Gameplay)) // technically a duplicate check, since role above should also validate this, but just to be safe and avoid any weird edge cases where somehow we get here without a valid user session, etc
			{
				UserSession? sourceData = WebSocketManager.GetSessionFromUser(user_id, sessionType);
				if (sourceData != null)
				{
					// TODO_QUICKMATCH: We need a way of checking if player is really in a match or not, so they cant just upload all the time, and also dont let them keep uploading replays if they already did, etc
					
					// lobby cant have AI and must have at least 2 human players at some point
					Lobby? lobby = _lobbyManager.GetLobby(sourceData.currentLobbyID);
					if (lobby == null || !lobby.WasPVPAtStart() || lobby.HadAIAtStart())
					{
						Response.StatusCode = (int)HttpStatusCode.NotAcceptable;
						return result;
					}

					try
					{
						using (var reader = new StreamReader(HttpContext.Request.Body))
						{
							string jsonData = await reader.ReadToEndAsync();

							var options = new JsonSerializerOptions
							{
								PropertyNameCaseInsensitive = true
							};

							var data = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(jsonData, options);

							if (data != null
								&& data.ContainsKey("replaydata")
								&& data.ContainsKey("match_id")
								)
							{
								string? b64ReplayData = data["replaydata"].GetString();
								if (b64ReplayData != null)
								{
									byte[] replayBytes = Convert.FromBase64String(b64ReplayData);
									UInt64 match_id = data["match_id"].GetUInt64();

									// were they really in the match they claim to be in?
									if (!sourceData.WasPlayerInMatch(match_id, out int slotIndexInLobby, out int army))
									{
										Response.StatusCode = (int)HttpStatusCode.Unauthorized;
										return result;
									}

                                    // try to queue it
                                    ES3QueueUploadResult queueResult = BackgroundS3Uploader.QueueUpload(ES3UploadType.Replay, replayBytes, match_id, user_id, slotIndexInLobby, EScreenshotType.NONE);
                                    if (queueResult == ES3QueueUploadResult.Success)
                                    {
                                        Response.StatusCode = (int)HttpStatusCode.OK;
                                    }
                                    else
                                    {
                                        Response.StatusCode = (int)HttpStatusCode.UnsupportedMediaType;
                                    }
								}
							}
						}
					}
					catch
					{
						Response.StatusCode = (int)HttpStatusCode.InternalServerError;
						return result;
					}
				}
			}

			return result;
		}
	}
}