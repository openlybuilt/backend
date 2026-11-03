/** Find the next selected local weekday/time, skipping gaps and choosing the first occurrence of an overlap. */
export function nextWeekly(
  after: Date,
  timezone: string,
  weekdays: number[],
  localTime: string,
): Date {
  const formatter = new Intl.DateTimeFormat("en-GB", {
    timeZone: timezone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hourCycle: "h23",
  });
  const wallAt = (instant: number) => {
    const parts = Object.fromEntries(
      formatter.formatToParts(instant).map((part) => [part.type, part.value]),
    );
    return Date.UTC(
      Number(parts.year),
      Number(parts.month) - 1,
      Number(parts.day),
      Number(parts.hour),
      Number(parts.minute),
    );
  };
  const localToday = new Date(wallAt(after.getTime()));
  const [hour, minute] = localTime.split(":").map(Number) as [number, number];
  const midnight = Date.UTC(
    localToday.getUTCFullYear(),
    localToday.getUTCMonth(),
    localToday.getUTCDate(),
  );
  for (let days = 0; days <= 14; days++) {
    const date = new Date(midnight + days * 86400000);
    if (!weekdays.includes(date.getUTCDay())) continue;
    const wall = date.getTime() + (hour * 60 + minute) * 60000;
    const offsets = new Set<number>();
    for (let hours = -48; hours <= 48; hours += 3) {
      const instant = wall + hours * 3600000;
      offsets.add(wallAt(instant) - instant);
    }
    const candidates = [...offsets]
      .map((offset) => wall - offset)
      .filter((instant) => wallAt(instant) === wall)
      .sort((a, b) => a - b);
    const first = candidates[0];
    if (first !== undefined && first > after.getTime()) return new Date(first);
  }
  throw new Error("Cannot determine the next weekly occurrence");
}
