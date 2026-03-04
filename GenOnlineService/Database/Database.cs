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

public class AppDbContext : DbContext
{
	public DbSet<User> Users => Set<User>();
	public DbSet<DailyStat> DailyStats => Set<DailyStat>();

	public AppDbContext(DbContextOptions<AppDbContext> options)
		: base(options)
	{
	}

	protected override void OnModelCreating(ModelBuilder modelBuilder)
	{
		// Fluent configuration here
		base.OnModelCreating(modelBuilder);

		modelBuilder.ApplyConfiguration(new UserConfiguration());
		modelBuilder.ApplyConfiguration(new DailyStatsConfiguration());
		modelBuilder.ApplyConfiguration(new LeaderboardDailyConfiguration());
		modelBuilder.ApplyConfiguration(new LeaderboardMonthlyConfiguration());
		modelBuilder.ApplyConfiguration(new LeaderboardYearlyConfiguration());
	}
}