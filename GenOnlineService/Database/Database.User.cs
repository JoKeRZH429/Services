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

using GenOnlineService;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using System.Text.Json;
using static Database.Functions.Auth;

public class PendingLogin
{
	public Int64 UserID { get; set; }

	public DateTime Created { get; set; } = DateTime.UnixEpoch;
	public EPendingLoginState State { get; set; } = EPendingLoginState.None;
	public string LoginCode { get; set; } = String.Empty;
}

public class UserDevice
{
	public Int64 UserID { get; set; }

	public string HWID_0 { get; set; } = String.Empty;
	public string HWID_1 { get; set; } = String.Empty;
	public string HWID_2 { get; set; } = String.Empty;
	public string HWID_3 { get; set; } = String.Empty;
	public string HWID_4 { get; set; } = String.Empty;
	public string HWID_5 { get; set; } = String.Empty;

	public string IPAddress { get; set; } = String.Empty;
}

public class User
{
	public Int64 ID { get; set; }
	public EAccountType AccountType { get; set; } = EAccountType.Unknown;

	// Steam, only present if AccountType is Steam
	public Int64 SteamID { get; set; } = -1;

	// Discord, only present if AccountType is Discord
	public Int64 DiscordID { get; set; } = -1;
	public string DiscordUsername { get; set; } = String.Empty;

	// GameReplays, only present if AccountType is GameReplays
	public Int64 GameReplaysID { get; set; } = -1;
	public string GameReplaysUsername { get; set; } = String.Empty;


	public string DisplayName { get; set; } = "";
	public DateTime LastLogin { get; set; } = DateTime.UnixEpoch;
	public string LastIPAddress { get; set; } = String.Empty;
	public int ClientID { get; set; } = -1;

	// Gameplay Favorites
	public int FavoriteColor { get; set; } = -1;
	public int FavoriteSide { get; set; } = -1;
	public string FavoriteMap { get; set; } = String.Empty;
	public int FavoriteStartingMoney { get; set; } = -1;
	public bool LimitSuperweapons { get; set; } = false;

	// User Permissions
	public bool IsAdmin { get; set; } = false;
	public bool IsBanned { get; set; } = false;

	// ELO
	public int EloRating { get; set; } = EloConfig.BaseRating;
	public int EloNumberOfMatches { get; set; } = 0;

	// Bans
	public string BanReason { get; set; } = String.Empty;
	public string BannedBy { get; set; } = String.Empty;
	public string BanVerifiedBy { get; set; } = String.Empty;
	public string BanAliases { get; set; } = String.Empty;
}

public class UserLobbyPreferences
{
	public int favorite_color = -1;
	public int favorite_side = -1;
	public string favorite_map = String.Empty;
	public int favorite_starting_money = -1;
	public bool favorite_limit_superweapons = false;
}

// TODO_EFCORE: add index for code
public class PendingLoginConfiguration : IEntityTypeConfiguration<PendingLogin>
{
	public void Configure(EntityTypeBuilder<PendingLogin> builder)
	{
		builder.ToTable("pending_logins");

		builder.Property(e => e.UserID).HasColumnName("user_id");
		builder.Property(e => e.LoginCode).HasColumnName("code").HasColumnType("varchar(32)");
		builder.Property(e => e.State).HasColumnName("state").HasColumnType("int(1)");
		builder.Property(e => e.Created).HasColumnName("created");
	}
}

public class UserDevicesConfiguration : IEntityTypeConfiguration<UserDevice>
{
	public void Configure(EntityTypeBuilder<UserDevice> builder)
	{
		builder.ToTable("user_devices");

		// prim key
		builder.HasKey(e => new { e.UserID, e.HWID_0, e.HWID_1, e.HWID_2, e.IPAddress});

		builder.Property(e => e.UserID).HasColumnName("user_id");
		builder.Property(e => e.HWID_0).HasColumnName("hwid_0").HasColumnType("varchar(128)");
		builder.Property(e => e.HWID_0).HasColumnName("hwid_1").HasColumnType("varchar(128)");
		builder.Property(e => e.HWID_0).HasColumnName("hwid_2").HasColumnType("varchar(128)");
		builder.Property(e => e.HWID_0).HasColumnName("hwid_3").HasColumnType("varchar(50)");
		builder.Property(e => e.HWID_0).HasColumnName("hwid_4").HasColumnType("varchar(50)");
		builder.Property(e => e.HWID_0).HasColumnName("hwid_5").HasColumnType("varchar(50)");
		builder.Property(e => e.HWID_0).HasColumnName("ip_addr").HasColumnType("varchar(45)");
	}
}

public class UserConfiguration : IEntityTypeConfiguration<User>
{
	public void Configure(EntityTypeBuilder<User> builder)
	{
		builder.ToTable("users");

		// prim key
		builder.HasKey(e => e.ID);

		builder.Property(e => e.ID).HasColumnName("user_id");

		builder.Property(e => e.AccountType).HasColumnName("account_type");
		builder.Property(e => e.SteamID).HasColumnName("steam_id");
		builder.Property(e => e.DiscordID).HasColumnName("discord_id");
		builder.Property(e => e.DiscordUsername).HasColumnName("discord_username").HasColumnType("varchar(32)"); ;
		builder.Property(e => e.GameReplaysID).HasColumnName("gamereplays_id");
		builder.Property(e => e.GameReplaysUsername).HasColumnName("gamereplays_username").HasColumnType("varchar(32)"); ;
		builder.Property(e => e.DisplayName).HasColumnName("displayname").HasColumnType("varchar(32)"); ;
		builder.Property(e => e.LastLogin).HasColumnName("lastlogin").HasColumnType("datetime(6)");
		builder.Property(e => e.LastIPAddress).HasColumnName("last_ip").HasColumnType("varchar(45)"); ;
		builder.Property(e => e.ClientID).HasColumnName("client_id");
		builder.Property(e => e.FavoriteColor).HasColumnName("favorite_color");
		builder.Property(e => e.FavoriteSide).HasColumnName("favorite_side");
		builder.Property(e => e.FavoriteMap).HasColumnName("favorite_map").HasColumnType("varchar(128)"); ;
		builder.Property(e => e.FavoriteStartingMoney).HasColumnName("favorite_starting_money");
		builder.Property(e => e.LimitSuperweapons).HasColumnName("favorite_limit_superweapons");
		builder.Property(e => e.IsAdmin).HasColumnName("admin");
		builder.Property(e => e.IsBanned).HasColumnName("banned");
		builder.Property(e => e.EloRating).HasColumnName("elo_rating");
		builder.Property(e => e.EloNumberOfMatches).HasColumnName("elo_num_matches");
		builder.Property(e => e.BanReason).HasColumnName("ban_reason").HasColumnType("varchar(128)"); ;
		builder.Property(e => e.BannedBy).HasColumnName("banned_by").HasColumnType("varchar(50)"); ;
		builder.Property(e => e.BanVerifiedBy).HasColumnName("ban_verified_by").HasColumnType("varchar(50)"); ;
		builder.Property(e => e.BanAliases).HasColumnName("ban_aliases").HasColumnType("varchar(50)"); ;
	}
}

namespace Database
{
	public static class PendingLogins
	{
		private static readonly Func<AppDbContext, string, Task<long?>> GetUserIdFromCode =
		EF.CompileAsyncQuery(
			(AppDbContext db, string code) =>
				db.PendingLogins
				  .Where(p => p.LoginCode == code)
				  .Select(p => (long?)p.UserID)
				  .FirstOrDefault()
		);

		public static async Task Cleanup(AppDbContext db, bool startup)
		{
			TimeSpan threshold = startup ? TimeSpan.FromSeconds(1) : TimeSpan.FromMinutes(5);

			DateTime cutoff = DateTime.UtcNow - threshold;

			await db.PendingLogins
				.Where(p => p.Created <= cutoff)
				.ExecuteDeleteAsync();
		}

		public static async Task<long> GetUserIDFromPendingLogin(AppDbContext db, string gameCode)
		{
			gameCode = gameCode.ToUpper();

			var result = await GetUserIdFromCode(db, gameCode);

			return result ?? -1;
		}

		public static async Task CleanupPendingLogin(AppDbContext db, string gameCode)
		{
			gameCode = gameCode.ToUpper();

			await db.PendingLogins
				.Where(p => p.LoginCode == gameCode)
				.ExecuteDeleteAsync();
		}

	}

	public static class UserDevices
	{
		public static readonly Func<AppDbContext, long, string, string, string, Task<UserDevice?>> FindDevice =
				EF.CompileAsyncQuery(
					(AppDbContext db, long userId, string h0, string h1, string h2) =>
						db.UserDevices.FirstOrDefault(d =>
							d.UserID == userId &&
							d.HWID_0 == h0 &&
							d.HWID_1 == h1 &&
							d.HWID_2 == h2)
				);

		public static async Task RegisterUserDevice(
			AppDbContext db,
			long userId,
			string hwid_0,
			string hwid_1,
			string hwid_2,
			string ipAddr)
		{
			// raw versions
			string hwid_3 = hwid_0.ToUpper();
			string hwid_4 = hwid_1.ToUpper();
			string hwid_5 = hwid_2.ToUpper();

			// hashed versions
			string h0 = Helpers.ComputeMD5Hash(hwid_0).ToUpper();
			string h1 = Helpers.ComputeMD5Hash(hwid_1).ToUpper();
			string h2 = Helpers.ComputeMD5Hash(hwid_2).ToUpper();

			// check if exists (precompiled query)
			var existing = await FindDevice(db, userId, h0, h1, h2);
			if (existing != null)
				return;

			// insert new (if doesnt exist)
			var device = new UserDevice
			{
				UserID = userId,
				HWID_0 = h0,
				HWID_1 = h1,
				HWID_2 = h2,
				HWID_3 = hwid_3,
				HWID_4 = hwid_4,
				HWID_5 = hwid_5,
				IPAddress = ipAddr
			};

			db.UserDevices.Add(device);
			await db.SaveChangesAsync();
		}
	}


	public static class Users
	{
		private static readonly Func<AppDbContext, long, Task<EloData?>> GetEloData =
				EF.CompileAsyncQuery(
					(AppDbContext db, long userId) =>
						db.Users
						  .Where(u => u.ID == userId)
						  .Select(u => new EloData(u.EloRating, u.EloNumberOfMatches))
						  .FirstOrDefault()
				);

		private static readonly Func<AppDbContext, long, Task<bool>> _isUserAdminQuery =
			EF.CompileAsyncQuery((AppDbContext db, long userId) =>
				db.Users
				  .AsNoTracking()
				  .Where(u => u.ID == userId)
				  .Select(u => u.IsAdmin)
				  .FirstOrDefault());

		private static readonly Func<AppDbContext, long, Task<bool>> _isUserBannedQuery =
			EF.CompileAsyncQuery((AppDbContext db, long userId) =>
				db.Users
				  .AsNoTracking()
				  .Where(u => u.ID == userId)
				  .Select(u => u.IsBanned)
				  .FirstOrDefault());

		private static readonly Func<AppDbContext, long, Task<string?>> _getDisplayNameQuery =
				EF.CompileAsyncQuery((AppDbContext db, long userId) =>
					db.Users
					  .AsNoTracking()
					  .Where(u => u.ID == userId)
					  .Select(u => u.DisplayName)
					  .FirstOrDefault()
				);

		private static readonly Func<AppDbContext, long, Task<UserLobbyPreferences?>> _getUserLobbyPreferencesQuery =
			EF.CompileAsyncQuery((AppDbContext db, long userId) =>
				db.Users
				  .AsNoTracking()
				  .Where(u => u.ID == userId)
				  .Select(u => new UserLobbyPreferences
				  {
					  favorite_color = u.FavoriteColor,
					  favorite_side = u.FavoriteSide,
					  favorite_map = u.FavoriteMap,
					  favorite_starting_money = u.FavoriteStartingMoney,
					  favorite_limit_superweapons = u.LimitSuperweapons
				  })
				  .FirstOrDefault()
			);

		private static readonly Func<AppDbContext, List<long>, IAsyncEnumerable<User>> _compiledBulkQuery =
			EF.CompileAsyncQuery(
				(AppDbContext db, List<long> ids) =>
					db.Users.Where(u => ids.Contains(u.ID))
			);


		public static async Task<Dictionary<long, EloData>> GetBulkELOData(
	AppDbContext db, List<long> userIds)
		{
			Dictionary<long, EloData> results = new();

			if (userIds == null || userIds.Count == 0)
				return results;

			// Execute compiled query
			await foreach (var u in _compiledBulkQuery(db, userIds))
			{
				results[u.ID] = new EloData(u.EloRating, u.EloNumberOfMatches);
			}

			// Fill missing users with defaults
			foreach (var id in userIds)
			{
				if (!results.ContainsKey(id))
					results[id] = new EloData(EloConfig.BaseRating, 0);
			}

			return results;
		}


#if DEBUG
		public static readonly Func<AppDbContext, long, Task<bool>> UserExists =
        EF.CompileAsyncQuery(
            (AppDbContext db, long userId) =>
                db.Users.Any(u => u.ID == userId)
        );

		internal static async Task CreateUserIfNotExists_DevAccount(
		AppDbContext db, long userId, string displayName)
		{
			// Normalize input
			displayName = displayName?.Trim();

			// Fast existence check
			bool exists = await UserExists(db, userId);

			if (!exists)
			{
				db.Users.Add(new User
				{
					ID = userId,
					AccountType = EAccountType.DevAccount,
					DisplayName = displayName
				});

				await db.SaveChangesAsync();
			}
		}

#endif

		public static Task<bool> IsUserAdmin(AppDbContext db, long userId)
		{
			return _isUserAdminQuery(db, userId);
		}

		public static Task<bool> IsUserBanned(AppDbContext db, long userId)
		{
			return _isUserBannedQuery(db, userId);
		}


		public static async Task<string> GetDisplayName(AppDbContext db, long userId)
		{
			return await _getDisplayNameQuery(db, userId) ?? string.Empty;
		}

		public static Task<UserLobbyPreferences?> GetUserLobbyPreferences(AppDbContext db, long userId)
		{
			return _getUserLobbyPreferencesQuery(db, userId);
		}

		// TODO_EFCORE: check all queries, determine which ones should be moved to precompiled query
		public static async Task SetFavorite_LimitSuperweapons(
			AppDbContext db,
			long userId,
			bool bLimitSuperweapons)
		{
			// TODO_EFCORE: Check all sets, some may want to be execute update instead of db.SaveChangesAsync(); as this requires a lookup first
			await db.Users.Where(u => u.ID == userId).ExecuteUpdateAsync(setters => setters.SetProperty(u => u.LimitSuperweapons, bLimitSuperweapons));
		}

		public static async Task<EloData> GetELOData(AppDbContext db, long userId)
		{
			var result = await GetEloData(db, userId);

			if (result != null)
				return result;

			return new EloData(EloConfig.BaseRating, 0);
		}


		public static async Task SetFavorite_Map(
			AppDbContext db,
			long userId,
			string strMap)
		{
			await db.Users.Where(u => u.ID == userId).ExecuteUpdateAsync(setters => setters.SetProperty(u => u.FavoriteMap, strMap));
		}

		public static async Task UpdateLastLoginData(AppDbContext db, long userId, string ipAddr)
		{
			await db.Users
				.Where(u => u.ID == userId)
				.ExecuteUpdateAsync(setters => setters
					.SetProperty(u => u.LastLogin, DateTime.UtcNow)
					.SetProperty(u => u.LastIPAddress, ipAddr)
				);
		}


		public static async Task SetFavorite_StartingMoney(
			AppDbContext db,
			long userId,
			int startingMoney)
		{
			await db.Users.Where(u => u.ID == userId).ExecuteUpdateAsync(setters => setters.SetProperty(u => u.FavoriteStartingMoney, startingMoney));
		}

		public static async Task SetFavorite_Side(
			AppDbContext db,
			long userId,
			int side)
		{
			await db.Users.Where(u => u.ID == userId).ExecuteUpdateAsync(setters => setters.SetProperty(u => u.FavoriteSide, side));
		}

		public static async Task SetDisplayName(AppDbContext db, long userId, string newName)
		{
			await db.Users
				.Where(u => u.ID == userId)
				.ExecuteUpdateAsync(setters => setters
					.SetProperty(u => u.DisplayName, newName)
				);
		}

		public static async Task SetFavorite_Color(
			AppDbContext db,
			long userId,
			int color)
		{
			await db.Users.Where(u => u.ID == userId).ExecuteUpdateAsync(setters => setters.SetProperty(u => u.FavoriteColor, color));
		}

		public static async Task SaveELOData(AppDbContext db, long userId, EloData newEloData)
		{
			await db.Users
				.Where(u => u.ID == userId)
				.ExecuteUpdateAsync(setters => setters
					.SetProperty(u => u.EloRating, newEloData.Rating)
					.SetProperty(u => u.EloNumberOfMatches, newEloData.NumMatches)
				);
		}

	}
}