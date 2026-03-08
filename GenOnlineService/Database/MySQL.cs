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

#define USE_PER_QUERY_CONNECTION

using Amazon.S3.Model;
using Discord;
using GenOnlineService;
using GenOnlineService.Controllers;
using Microsoft.AspNetCore.Connections.Features;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Hosting;
using MySql.Data.MySqlClient;
using MySqlX.XDevAPI;
using MySqlX.XDevAPI.Common;
using Sentry.Protocol;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Drawing;
using System.IO;
using System.Net;
using System.Net.WebSockets;
using System.Numerics;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Security.Policy;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using static Database.Functions;
using static Database.Functions.Auth;
using static Database.Functions.Lobby;







/*
 * 2, // USA
        3, // CHINA
        4, // GLA
        5, // USA Super Weapon
        6, // USA Laser
        7, // USA Airforce
        8, // China Tank
        9, // China Infantry
        10, // China Nuke
        11, // GLA Toxin
        12, // GLA Demo
        13 // GLA Stealth
*/



namespace Database
{
	public static class Functions
	{
			public static class Lobby
		{

			// Called when a lobby is deleted, thats the true end of a match
			public async static Task CommitLobbyToMatchHistory(MySQLInstance m_Inst, GenOnlineService.Lobby lobby)
			{
				if (lobby.MatchID != 0) // 0 is invalid
				{
					await m_Inst.Query("UPDATE match_history SET finished=true, time_finished=current_timestamp() WHERE match_id=@match_id AND finished=false LIMIT 1;",
					new()
					{
						{ "@match_id", lobby.MatchID }
					});
				}
			}

			public enum EScreenshotType
			{
				NONE = -1,
				SCREENSHOT_TYPE_LOADSCREEN = 0,
				SCREENSHOT_TYPE_GAMEPLAY = 1,
				SCREENSHOT_TYPE_SCORESCREEN = 2
			}
			

			public enum EMetadataFileType
			{
				UNKNOWN = -1,
				FILE_TYPE_SCREENSHOT = 0,
				FILE_TYPE_REPLAY = 1
			};

			public async static Task AttachMatchHistoryMetadata(MySQLInstance m_Inst, UInt64 MatchID, int slotIndex, string strVal, EMetadataFileType fileType)
			{
				if (MatchID != 0) // 0 is invalid
				{
					if (slotIndex < 0)
					{
						return;
					}

					CMySQLResult resMember = await m_Inst.Query(String.Format("UPDATE match_history SET member_slot_{0} = JSON_ARRAY_APPEND(member_slot_{0}, '$.metadata', JSON_OBJECT('file_name', @file_name, 'file_type', @file_type)) WHERE match_id = @match_id;", slotIndex),
						new()
						{
						{ "@match_id", MatchID },
						{ "@file_name", strVal },
						{ "@file_type", (int)fileType },
						}
					);
				}
			}

			public class IntToBoolConverter : JsonConverter<bool>
			{
				public override bool Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
				{
					return reader.GetInt32() != 0;
				}

				public override void Write(Utf8JsonWriter writer, bool value, JsonSerializerOptions options)
				{
					writer.WriteNumberValue(value ? 1 : 0);
				}
			}
		}

		// TODO: Cleanup things when a user disconnects, e.g. lobby they're in etc
		public static class Auth
		{
			public async static Task UpdatePlayerStat(MySQLInstance m_Inst, Int64 user_id, int stat_id, int stat_val)
			{
				await m_Inst.Query("INSERT INTO user_stats_v2 (user_id, stats) VALUES (@user_id, JSON_OBJECT(@stat_key_raw, @stat_val)) ON DUPLICATE KEY UPDATE stats = JSON_SET(stats, @stat_key_formatted, @stat_val);",
					new()
					{
						{ "@user_id", user_id },
						{ "@stat_key_raw", stat_id },
						{ "@stat_key_formatted", String.Format("$.{0}", stat_id) },
						{ "@stat_val", stat_val }
					}
				);
			}

			public async static Task StoreConnectionOutcome(MySQLInstance m_Inst, EIPVersion protocol, EConnectionState outcome)
			{
				if (outcome != EConnectionState.CONNECTED_DIRECT && outcome != EConnectionState.CONNECTED_RELAY && outcome != EConnectionState.CONNECTION_FAILED) // states we dont track
				{
					return;
				}

				// increment count
				int day_of_year = DateTime.Now.DayOfYear;

				// these are used for creation, so we need to determine 0 1, if already exists, we increment instead
				int create_ipv4_count = protocol == EIPVersion.IPV4 ? 1 : 0;
				int create_ipv6_count = protocol == EIPVersion.IPV6 ? 1 : 0;
				int create_success_count = (outcome == EConnectionState.CONNECTED_DIRECT || outcome == EConnectionState.CONNECTED_RELAY) ? 1 : 0;
				int create_failed_count = (outcome == EConnectionState.CONNECTION_FAILED) ? 1 : 0;

				string onDupeAction = "";

				// what action do we want?
				if (protocol == EIPVersion.IPV4)
				{
					if (onDupeAction.Length > 0)
					{
						onDupeAction += ", ";
					}

					onDupeAction += "ipv4_count=ipv4_count+1";
				}
				else if (protocol == EIPVersion.IPV6)
				{
					if (onDupeAction.Length > 0)
					{
						onDupeAction += ", ";
					}

					onDupeAction += "ipv6_count=ipv6_count+1";
				}

				// 2nd part of action
				if (outcome == EConnectionState.CONNECTED_DIRECT || outcome == EConnectionState.CONNECTED_RELAY)
				{
					if (onDupeAction.Length > 0)
					{
						onDupeAction += ", ";
					}
					onDupeAction += "success_count=success_count+1";
				}
				else if (outcome == EConnectionState.CONNECTION_FAILED)
				{
					if (onDupeAction.Length > 0)
					{
						onDupeAction += ", ";
					}
					onDupeAction += "failed_count=failed_count+1";
				}

				await m_Inst.Query(String.Format("INSERT INTO connection_outcomes SET day_of_year=@day_of_year, ipv4_count=@ipv4_count, ipv6_count=@ipv6_count, success_count=@success_count, failed_count=@failed_count ON DUPLICATE KEY UPDATE {0};", onDupeAction),
					new()
					{
					{ "@day_of_year", day_of_year },
					{ "@ipv4_count", create_ipv4_count },
					{ "@ipv6_count", create_ipv6_count },
					{ "@success_count", create_success_count },
					{ "@failed_count", create_failed_count }
					}
				);

				// TODO_URGENT: Handle year roll over
				await m_Inst.Query("DELETE FROM connection_outcomes WHERE day_of_year<(@day_of_year - 30);",
					new()
					{
						{ "@day_of_year", day_of_year }
					}
				);
			}

			public async static Task<PlayerStats> GetPlayerStats(AppDbContext _db, MySQLInstance m_Inst, Int64 user_id)
			{
				// TODO: Return null if user doesnt actually exist, instead of empty stats
				EloData eloData = await Database.Users.GetELOData(_db, user_id);
                PlayerStats ps = new PlayerStats(user_id, eloData.Rating, eloData.NumMatches);

                var res = await m_Inst.Query("SELECT stats FROM user_stats_v2 WHERE user_id=@user_id LIMIT 1;",
				new()
				{
					{ "@user_id", user_id }
				}
				);

				if (res.NumRows() == 0)
				{
					return ps;
				}

				string? jsonData = Convert.ToString(res.GetRow(0)["stats"]);
#pragma warning disable CS8600 // Converting null literal or possible null value to non-nullable type.
#pragma warning disable CS8604 // Converting null literal or possible null value to non-nullable type.
				Dictionary<int, int> dictStats = JsonSerializer.Deserialize<Dictionary<int, int>>(jsonData);
#pragma warning restore CS8604 // Converting null literal or possible null value to non-nullable type.
#pragma warning restore CS8600 // Converting null literal or possible null value to non-nullable type.

				//foreach (var row in res.GetRows())
#pragma warning disable CS8602 // Dereference of a possibly null reference.
				foreach (var statPair in dictStats)
				{
					EStatIndex stat_id = (EStatIndex)Convert.ToUInt16(statPair.Key);
					int stat_value = statPair.Value;

					ps.ProcessFromDB(stat_id, stat_value);
				}
#pragma warning restore CS8602 // Dereference of a possibly null reference.

				return ps;
			}

			public static async Task FullyDestroyPlayerSession(MySQLInstance m_Inst, Int64 user_id, UserSession? userData, bool bMigrateLobbyIfPresent)
			{
				// NOTE: Dont assume userData is valid, use user_id for user id
				Console.ForegroundColor = ConsoleColor.Cyan;
				Console.WriteLine("FullyDestroyPlayerSession for user {0}", user_id);
				Console.ForegroundColor = ConsoleColor.Gray;

				// invalidate any TURN credentials
				TURNCredentialManager.DeleteCredentialsForUser(user_id);

				// TODO: Implement single point of presence? gets dicey if multiple logins
				// TODO: Dont destroy this, just mark inactive/offline, we use this as a saved credential system

				// session tied to this token (keep other ones attached to user_id, could be other machines)
				// TODO_JWT: Remove table fully + set logged out
				//await m_Inst.Query("DELETE FROM sessions WHERE user_id={0} AND session_type={1};", user_id, (int)ESessionType.Game);

				// leave any lobby
				Console.WriteLine("[Source 2] User {0} Leave Any Lobby", user_id);

				var lobbyManager = ServiceLocator.Services.GetRequiredService<LobbyManager>();
				lobbyManager.LeaveAnyLobby(user_id);


				await lobbyManager.CleanupUserLobbiesNotStarted(user_id);

				// remove from any matchmaking
				if (userData != null)
				{
					MatchmakingManager.DeregisterPlayer(userData);
				}

				// TODO: Client needs to handle this... itll start returning 404
			}

			public async static Task SetUsedLoggedIn(MySQLInstance m_Inst, Int64 userID, KnownClients.EKnownClients clientID, EUserSessionType sessionType)
			{
				// TODO_EFCORE: website uses this index as 1 (60hz) to 0 (30hz), update it to use new enum + support new clients, also need to update DB to match
				// TODO_EFCORE: Move away from db for this and just have website login call endpoint on service
				//UInt16 clientID = clientIDStr == "gen_online_60hz" ? (UInt16)1 : (UInt16)0;

				Console.ForegroundColor = ConsoleColor.Cyan;
				Console.WriteLine("StartSession deleing other sessions for user {0}", userID);
				Console.ForegroundColor = ConsoleColor.Gray;

				// kill any WS they had too, StartSession comes before WS connects
				// disconnect any other sessions with this ID
				UserSession? sess = GenOnlineService.WebSocketManager.GetSessionFromUser(userID, sessionType);
				if (sess != null)
				{
					Console.ForegroundColor = ConsoleColor.Cyan;
					Console.WriteLine("Found duplicate session for user {0}", userID);
					Console.ForegroundColor = ConsoleColor.Gray;

					UserWebSocketInstance? oldWS = GenOnlineService.WebSocketManager.GetWebSocketForSession(sess);
					await GenOnlineService.WebSocketManager.DeleteSession(userID, sessionType, oldWS, false);
				}
			}

			public async static Task<HashSet<Int64>> GetFriends(MySQLInstance m_Inst, Int64 user_id)
			{
				HashSet<Int64> setFriends = new();

				var res = await m_Inst.Query("SELECT user_id_1, user_id_2 FROM friends WHERE user_id_1=@user_id OR user_id_2=@user_id;",
				new()
				{
					{ "@user_id", user_id }
				}
				);

				foreach (var row in res.GetRows())
				{
					Int64 user_id_1 = Convert.ToInt64(row["user_id_1"]);
					Int64 user_id_2 = Convert.ToInt64(row["user_id_2"]);

					if (user_id_1 == user_id)
					{
						setFriends.Add(user_id_2);
					}
					else
					{
						setFriends.Add(user_id_1);
					}

				}

				return setFriends;
			}

			public async static Task<HashSet<Int64>> GetBlocked(MySQLInstance m_Inst, Int64 source_user_id)
			{
				HashSet<Int64> setBlocked = new();

				var res = await m_Inst.Query("SELECT target_user_id FROM friends_blocked WHERE source_user_id=@source_user_id;",
				new()
				{
					{ "@source_user_id", source_user_id }
				}
				);

				foreach (var row in res.GetRows())
				{
					Int64 blocked_user_id = Convert.ToInt64(row["target_user_id"]);
					setBlocked.Add(blocked_user_id);
				}

				return setBlocked;
			}

			public async static Task<HashSet<Int64>> GetPendingFriendsRequests(MySQLInstance m_Inst, Int64 target_user_id)
			{
				HashSet<Int64> setRequests = new();

				var res = await m_Inst.Query("SELECT source_user_id FROM friends_requests WHERE target_user_id=@target_user_id;",
				new()
				{
					{ "@target_user_id", target_user_id }
				}
				);

				foreach (var row in res.GetRows())
				{
					Int64 source_user_id = Convert.ToInt64(row["source_user_id"]);
					setRequests.Add(source_user_id);
				}

				return setRequests;
			}

			public async static Task RemovePendingFriendRequest(MySQLInstance m_Inst, Int64 source_user_id, Int64 target_user_id)
			{
				// delete in either direction
				var res = await m_Inst.Query("DELETE FROM friends_requests WHERE (source_user_id=@source_user_id AND target_user_id=@target_user_id) OR (source_user_id=@target_user_id AND target_user_id=@source_user_id) LIMIT 1;",
				new()
				{
					{ "@source_user_id", source_user_id },
					{ "@target_user_id", target_user_id }
				}
				);
			}

			public async static Task CreateFriendship(MySQLInstance m_Inst, Int64 source_user_id, Int64 target_user_id)
			{
				var res = await m_Inst.Query("INSERT INTO friends(user_id_1, user_id_2) VALUES (@source_user_id, @target_user_id);",
				new()
				{
					{ "@source_user_id", source_user_id },
					{ "@target_user_id", target_user_id }
				}
				);
			}

			public async static Task RemoveFriendship(MySQLInstance m_Inst, Int64 source_user_id, Int64 target_user_id)
			{
				var res = await m_Inst.Query("DELETE FROM friends WHERE (user_id_1=@source_user_id AND user_id_2=@target_user_id) OR  (user_id_1=@target_user_id AND user_id_2=@source_user_id ) LIMIT 1;",
				new()
				{
					{ "@source_user_id", source_user_id },
					{ "@target_user_id", target_user_id }
				}
				);
			}

			public async static Task AddBlock(MySQLInstance m_Inst, Int64 source_user_id, Int64 target_user_id)
			{
				var res = await m_Inst.Query("INSERT INTO friends_blocked(source_user_id, target_user_id) VALUES (@source_user_id, @target_user_id);",
				new()
				{
					{ "@source_user_id", source_user_id },
					{ "@target_user_id", target_user_id }
				}
				);
			}

			public async static Task RemoveBlock(MySQLInstance m_Inst, Int64 source_user_id, Int64 target_user_id)
			{
				var res = await m_Inst.Query("DELETE FROM friends_blocked WHERE source_user_id=@source_user_id AND target_user_id=@target_user_id LIMIT 1;",
				new()
				{
					{ "@source_user_id", source_user_id },
					{ "@target_user_id", target_user_id }
				}
				);
			}


			public async static Task AddPendingFriendRequest(MySQLInstance m_Inst, Int64 source_user_id, Int64 target_user_id)
			{
				var res = await m_Inst.Query("INSERT INTO friends_requests(source_user_id, target_user_id) VALUES (@source_user_id, @target_user_id);",
				new()
				{
					{ "@source_user_id", source_user_id },
					{ "@target_user_id", target_user_id }
				}
				);
			}

			public enum EAccountType
			{
				Unknown = -1,
				Steam = 0,
				Discord = 1,
				Ghost = 2,
				DevAccount = 3
			}
		}
	}

	// Updated MySQLInstance class to fix memory leaks by ensuring proper disposal of resources.
	public class MySQLInstance : IDisposable
	{
		// Connection string is built once from config and reused across all concurrent queries.
		// The MySQL connector's built-in connection pool (MySqlConnection with Pooling=true) is
		// fully thread-safe: each call to OpenAsync() leases an independent physical connection
		// from the pool, so queries on different threads never share a connection object.
		private static string? _cachedConnectionString;
		private static readonly object _connStringLock = new object();

		private static string GetConnectionString()
		{
			if (_cachedConnectionString != null)
				return _cachedConnectionString;

			lock (_connStringLock)
			{
				if (_cachedConnectionString != null)
					return _cachedConnectionString;

				if (Program.g_Config == null)
					throw new Exception("Config is null. Check config file exists.");

				IConfiguration? dbSettings = Program.g_Config.GetSection("Database");
				if (dbSettings == null)
					throw new Exception("Database section in config is null / not set in config");

				string? db_host     = dbSettings.GetValue<string>("db_host")     ?? throw new Exception("DB Hostname is null / not set in config");
				string? db_name     = dbSettings.GetValue<string>("db_name")     ?? throw new Exception("DB Name is null / not set in config");
				string? db_username = dbSettings.GetValue<string>("db_username") ?? throw new Exception("DB Username is null / not set in config");
				string? db_password = dbSettings.GetValue<string>("db_password") ?? throw new Exception("DB Password is null / not set in config");
				ushort  db_port     = dbSettings.GetValue<ushort>("db_port");

				int  db_min_poolsize     = dbSettings.GetValue<int?>("db_min_poolsize")     ?? 50;
				int  db_max_poolsize     = dbSettings.GetValue<int?>("db_max_poolsize")     ?? 500;
				bool db_use_pooling      = dbSettings.GetValue<bool?>("db_use_pooling")     ?? true;
				bool db_conn_reset       = dbSettings.GetValue<bool?>("db_conn_reset")      ?? true;
				int  db_connect_timeout  = dbSettings.GetValue<int?>("db_connect_timeout")  ?? 10;
				int  db_command_timeout  = dbSettings.GetValue<int?>("db_command_timeout")  ?? 10;

				_cachedConnectionString = string.Format(
					"Server={0}; database={1}; user={2}; password={3}; port={4};" +
					"Pooling={5};DefaultCommandTimeout={9};Connect Timeout={10};" +
					"MinimumPoolSize={6};maximumpoolsize={7};AllowUserVariables=true;ConnectionReset={8};",
					db_host, db_name, db_username, db_password, db_port,
					db_use_pooling, db_min_poolsize, db_max_poolsize, db_conn_reset,
					db_command_timeout, db_connect_timeout);

				return _cachedConnectionString;
			}
		}

#if !USE_PER_QUERY_CONNECTION
        private MySqlConnection m_Connection = null;
#endif

		public MySQLInstance()
		{

		}

		public void Dispose()
		{
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		protected virtual void Dispose(bool disposing)
		{
			if (disposing)
			{
#if !USE_PER_QUERY_CONNECTION
                if (m_Connection != null)
                {
                    m_Connection.Dispose();
                    m_Connection = null;
                }
#endif
			}
		}

		// Written with Interlocked so concurrent threads don't race on a shared DateTime field.
		private long m_LastQueryTimeTicks = DateTime.Now.Ticks;

		public async Task<bool> Initialize(WebApplicationBuilder builder, bool bIsStartup = true)
		{
			if (Program.g_Config == null)
			{
				throw new Exception("Config is null. Check config file exists.");
			}

			IConfiguration? dbSettings = Program.g_Config.GetSection("Database");

			if (dbSettings == null)
			{
				throw new Exception("Database section in config is null / not set in config");
			}

			string? hostname = dbSettings.GetValue<string>("db_host");
			string? dbname = dbSettings.GetValue<string>("db_name");
			string? username = dbSettings.GetValue<string>("db_username");
			string? password = dbSettings.GetValue<string>("db_password");
			UInt16? port = dbSettings.GetValue<UInt16>("db_port");

			int? db_min_poolsize = dbSettings.GetValue<int>("db_min_poolsize");
			int? db_max_poolsize = dbSettings.GetValue<int>("db_max_poolsize");
			bool? db_use_pooling = dbSettings.GetValue<bool>("db_use_pooling");
			bool? db_conn_reset = dbSettings.GetValue<bool>("db_conn_reset");
			int? db_connect_timeout = dbSettings.GetValue<int>("db_connect_timeout");
			int? db_command_timeout = dbSettings.GetValue<int>("db_command_timeout");

			if (hostname == null)
			{
				throw new Exception("DB Hostname is null / not set in config");
			}

			if (dbname == null)
			{
				throw new Exception("DB Hostname is null / not set in config");
			}

			if (username == null)
			{
				throw new Exception("DB Hostname is null / not set in config");
			}

			if (password == null)
			{
				throw new Exception("DB Hostname is null / not set in config");
			}

			if (port == null)
			{
				throw new Exception("DB Hostname is null / not set in config");
			}


			if (!Directory.Exists("Exceptions"))
			{
				Directory.CreateDirectory("Exceptions");
			}

			// EFCore connect
			{
				var csb = new MySqlConnectionStringBuilder
				{
					Server = hostname,
					Port = (uint)port,
					Database = dbname,
					UserID = username,
					Password = password,
					ConnectionTimeout = (uint)db_connect_timeout,
					DefaultCommandTimeout = (uint)db_command_timeout,
					SslMode = MySqlSslMode.Preferred
				};

				// TODO_EFCORE: Consider use of ExecuteDeleteAsync and options.UseQueryTrackingBehavior(QueryTrackingBehavior.NoTracking);
				builder.Services.AddPooledDbContextFactory<AppDbContext>(options =>
				{
					options.UseMySql(
						csb.ConnectionString,
						ServerVersion.AutoDetect(csb.ConnectionString));

					options.UseQueryTrackingBehavior(QueryTrackingBehavior.NoTracking);

				});

			}

			try
			{
				Encoding.RegisterProvider(System.Text.CodePagesEncodingProvider.Instance);

#if !USE_PER_QUERY_CONNECTION
				//m_Connection = new MySqlConnection(String.Format("Server={0}; database={1}; user={2}; password={3}; port={4};Pooling=true;Connect Timeout=10;MinimumPoolSize=1;maximumpoolsize=100;AllowUserVariables=true;ConnectionReset=false;SslMode=Required;", dbSettings));
				m_Connection = new MySqlConnection(String.Format("Server={0}; database={1}; user={2}; password={3}; port={4};Pooling=true;Connect Timeout=10;MinimumPoolSize=1;maximumpoolsize=100;AllowUserVariables=true;ConnectionReset=false;", hostname, dbname, username, password, port));

				//Console.WriteLine(String.Format("Server={0}; database={1}; user={2}; password={3}; port={4};Pooling=true;Connect Timeout=100;MinimumPoolSize=1;maximumpoolsize=100;AllowUserVariables=true;ConnectionReset=false;SslMode=Required;", dbSettings));

				Console.WriteLine("Connecting to DB...");
				await m_Connection.OpenAsync().ConfigureAwait(false);

				Console.WriteLine("Connected to: " + m_Connection.ServerVersion);


				Console.WriteLine("MySQL Initialized");

				var t = Database.Functions.Lobby.GetAllLobbyInfo(this, 0, true, true, true, true, true);

				List<LobbyData> lstLobbies = await t;
#endif

				return true;
			}
			catch (MySqlException ex)
			{
				Console.WriteLine(ex.ToString());
				HandleMySqlException(ex, bIsStartup);
				return false;
			}
			catch (InvalidOperationException ex)
			{
				Console.WriteLine(ex.ToString());
				Console.WriteLine("MySQL Connection Failed. Potentially Malformed Connection String.");
				if (bIsStartup)
				{
					Console.WriteLine("\tPress any key to exit");
					Console.Read();
					Environment.Exit(1);
				}
				return false;
			}
			catch (Exception e)
			{
				Console.WriteLine(e.ToString());
				Console.WriteLine("\tPress any key to exit");
				return false;
			}
		}

		private void HandleMySqlException(MySqlException ex, bool bIsStartup)
		{
			File.WriteAllText(Path.Combine("Exceptions", "MYSQL_1_" + DateTime.Now.ToString("yyyyMMdd_HHmmss_fff") + ".txt"), ex.ToString());

			switch (ex.Number)
			{
				case 0:
					Console.WriteLine("MySQL Connection Failed. Cannot Connect to Server.");
					break;
				case 1:
					Console.WriteLine("MySQL Connection Failed. Invalid username/password.");
					break;
				case 1042:
					Console.WriteLine("MySQL Connection Failed. Connection Timed Out.");
					break;
			}

			if (bIsStartup)
			{
				Console.WriteLine("\tFATAL ERROR, Press any key to exit");
				Console.Read();
				Environment.Exit(1);
			}
		}

		private string EscapeAllAndFormatQuery(string strQuery, params object[] formatParams)
		{
			for (int i = 0; i < formatParams.Length; ++i)
			{
				if (formatParams[i].GetType() == typeof(string))
				{
					formatParams[i] = MySqlHelper.EscapeString((string)formatParams[i]);
				}
				else if (formatParams[i].GetType().IsEnum)
				{
					formatParams[i] = (int)formatParams[i];
				}
			}

			return String.Format(strQuery, formatParams);
		}

		public async Task<CMySQLResult> Query(string commandStr, Dictionary<string, object>? dictCommandValues, int attempt = 0)
		{
			// After 3 attempts, give up.
			if (attempt >= 3)
				return new CMySQLResult(0);

			Interlocked.Exchange(ref m_LastQueryTimeTicks, DateTime.Now.Ticks);

			// Each call opens its own connection leased from the shared pool.
			// No serializing lock is needed: MySqlConnection instances are never shared between callers.
			try
			{
				using (var connection = new MySqlConnection(GetConnectionString()))
				{
					await connection.OpenAsync().ConfigureAwait(false);

					try
					{
						using (var command = new MySqlCommand(commandStr, connection))
						{
							if (dictCommandValues != null)
							{
								foreach (var kvPair in dictCommandValues)
									command.Parameters.AddWithValue(kvPair.Key, kvPair.Value);
							}

							if (commandStr.ToUpper().StartsWith("DELETE") || commandStr.ToUpper().StartsWith("UPDATE"))
							{
								int numRowsModified = await command.ExecuteNonQueryAsync().ConfigureAwait(false);
								return new CMySQLResult(numRowsModified);
							}
							else
							{
								using (System.Data.Common.DbDataReader reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
								{
									return new CMySQLResult(reader, (ulong)command.LastInsertedId);
								}
							}
						}
					}
					catch (InvalidOperationException e)
					{
						string strExceptionMsg = e.InnerException != null ? e.InnerException.ToString() : e.Message;
						Console.WriteLine("MySQL Query Error (will retry): {0}", strExceptionMsg);
						File.WriteAllText(Path.Combine("Exceptions", "MYSQL_2_" + DateTime.Now.ToString("yyyyMMdd_HHmmss_fff") + ".txt"), "MySQL Query Error:" + strExceptionMsg);

						// The pool will surface a fresh physical connection on the next attempt.
						return await Query(commandStr, dictCommandValues, attempt + 1).ConfigureAwait(false);
					}
					catch (MySqlException ex)
					{
						Console.WriteLine(ex.ToString());
						HandleMySqlException(ex, false);
					}
					catch (Exception e)
					{
						string strErrorMsg = string.Format("MySQL Query Error: {0}", e.Message);
						Console.WriteLine(strErrorMsg);
						File.WriteAllText(Path.Combine("Exceptions", "MYSQL_3_" + DateTime.Now.ToString("yyyyMMdd_HHmmss_fff") + ".txt"), strErrorMsg);

						if (System.Diagnostics.Debugger.IsAttached)
							throw;
					}
				}
			}
			catch (Exception e)
			{
				string strErrorMsg = string.Format("MySQL Query Error: {0}", e.Message);
				Console.WriteLine(strErrorMsg);
				File.WriteAllText(Path.Combine("Exceptions", "MYSQL_4_" + DateTime.Now.ToString("yyyyMMdd_HHmmss_fff") + ".txt"), strErrorMsg);
			}

			return new CMySQLResult(0);
		}
	}

	public class CMySQLResult
	{
		public CMySQLResult(int rowsAffected)
		{
			m_RowsAffected = rowsAffected;
		}

		public CMySQLResult(System.Data.Common.DbDataReader dbReader, ulong InsertID)
		{
			try
			{
				while (dbReader.Read())
				{
					CMySQLRow thisRow = new CMySQLRow();
					for (int i = 0; i < dbReader.FieldCount; i++)
					{
						object? value = !dbReader.IsDBNull(i) ? dbReader.GetValue(i) : null;
						string fieldName = dbReader.GetName(i);
						thisRow[fieldName] = value;
					}
					m_Rows.Add(thisRow);
				}
			}
			finally
			{
				dbReader.Close();
				dbReader.Dispose(); // Ensure proper disposal
			}

			m_InsertID = InsertID;
			m_RowsAffected = 0;
		}

		public List<CMySQLRow> GetRows()
		{
			return m_Rows;
		}

		public CMySQLRow GetRow(int a_Index)
		{
			return m_Rows[a_Index];
		}

		public int NumRows()
		{
			return m_Rows.Count;
		}

		public ulong GetInsertID()
		{
			return m_InsertID;
		}

		public int GetNumRowsAffected()
		{
			return m_RowsAffected;
		}

		private List<CMySQLRow> m_Rows = new List<CMySQLRow>();
		private readonly ulong m_InsertID = 0;
		private readonly int m_RowsAffected = 0;
	}
}
