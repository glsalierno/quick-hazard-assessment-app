const casPattern = /^(\d{1,9})-(\d{2})-(\d)$/;

export function isValidCasFormat(value: string): boolean {
  return casPattern.test(value.trim());
}

export function normalizeCasInput(raw: string): string {
  const value = raw.trim();
  if (!value) {
    return '';
  }

  for (const part of value.split(/\s+/)) {
    const candidate = part.replace(/^[(),]+|[(),]+$/g, '');
    if (casPattern.test(candidate)) {
      return candidate;
    }
  }

  return value;
}

export function validateCasChecksum(cas: string): { valid: boolean; normalized: string } {
  const match = cas.trim().match(casPattern);
  if (!match) {
    return { valid: false, normalized: cas.trim() };
  }

  const [, first, second, check] = match;
  const digits = `${first}${second}${check}`;
  const expected = digits
    .split('')
    .reduce((total, digit, index) => total + Number(digit) * (digits.length - index), 0) % 10;

  return { valid: check === String(expected), normalized: `${first}-${second}-${check}` };
}

export function inputTypeForQuery(query: string): 'cas' | 'name' {
  return isValidCasFormat(query) ? 'cas' : 'name';
}
