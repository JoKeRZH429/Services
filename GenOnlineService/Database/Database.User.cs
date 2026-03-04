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

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using System.Text.Json;
using static Database.Functions.Auth;
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
		builder.Property(e => e.BanAliases).HasColumnName("ban_alises").HasColumnType("varchar(50)"); ;
	}
}

namespace Database
{
	public static class Users
	{
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
	}
}