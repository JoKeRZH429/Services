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

public class LeaderboardDaily
{
	public long UserId { get; set; }
	public int? Points { get; set; }
	public int DayOfYear { get; set; }
	public int Year { get; set; }
	public int? Wins { get; set; }
	public int? Losses { get; set; }
}

public class LeaderboardMonthly
{
	public long UserId { get; set; }
	public int? Points { get; set; }
	public int MonthOfYear { get; set; }
	public int Year { get; set; }
	public int? Wins { get; set; }
	public int? Losses { get; set; }
}

public class LeaderboardYearly
{
	public long UserId { get; set; }
	public int? Points { get; set; }
	public int Year { get; set; }
	public int? Wins { get; set; }
	public int? Losses { get; set; }
}

public class LeaderboardDailyConfiguration : IEntityTypeConfiguration<LeaderboardDaily>
{
	public void Configure(EntityTypeBuilder<LeaderboardDaily> builder)
	{
		builder.ToTable("leaderboard_daily");

		builder.HasKey(x => new { x.UserId, x.DayOfYear, x.Year });

		builder.Property(x => x.UserId)
			.HasColumnName("user_id")
			.IsRequired();

		builder.Property(x => x.Points)
			.HasColumnName("points");

		builder.Property(x => x.DayOfYear)
			.HasColumnName("day_of_year")
			.IsRequired();

		builder.Property(x => x.Year)
			.HasColumnName("year")
			.IsRequired();

		builder.Property(x => x.Wins)
			.HasColumnName("wins");

		builder.Property(x => x.Losses)
			.HasColumnName("losses");
	}
}

public class LeaderboardMonthlyConfiguration : IEntityTypeConfiguration<LeaderboardMonthly>
{
	public void Configure(EntityTypeBuilder<LeaderboardMonthly> builder)
	{
		builder.ToTable("leaderboard_monthly");

		builder.HasKey(x => new { x.UserId, x.MonthOfYear, x.Year });

		builder.Property(x => x.UserId)
			.HasColumnName("user_id")
			.IsRequired();

		builder.Property(x => x.Points)
			.HasColumnName("points");

		builder.Property(x => x.MonthOfYear)
			.HasColumnName("month_of_year")
			.IsRequired();

		builder.Property(x => x.Year)
			.HasColumnName("year")
			.IsRequired();

		builder.Property(x => x.Wins)
			.HasColumnName("wins");

		builder.Property(x => x.Losses)
			.HasColumnName("losses");
	}
}

public class LeaderboardYearlyConfiguration : IEntityTypeConfiguration<LeaderboardYearly>
{
	public void Configure(EntityTypeBuilder<LeaderboardYearly> builder)
	{
		builder.ToTable("leaderboard_yearly");

		builder.HasKey(x => new { x.UserId, x.Year });

		builder.Property(x => x.UserId)
			.HasColumnName("user_id")
			.IsRequired();

		builder.Property(x => x.Points)
			.HasColumnName("points");

		builder.Property(x => x.Year)
			.HasColumnName("year")
			.IsRequired();

		builder.Property(x => x.Wins)
			.HasColumnName("wins");

		builder.Property(x => x.Losses)
			.HasColumnName("losses");
	}
}
