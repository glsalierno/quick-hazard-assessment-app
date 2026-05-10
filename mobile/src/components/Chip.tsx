import { StyleSheet, Text, View } from 'react-native';

import { colors, radius, spacing } from '../constants/theme';

export function Chip({ label, tone = 'info' }: { label: string; tone?: 'info' | 'warning' | 'danger' }) {
  return (
    <View style={[styles.chip, tone === 'warning' && styles.warning, tone === 'danger' && styles.danger]}>
      <Text style={[styles.text, tone === 'warning' && styles.warningText, tone === 'danger' && styles.dangerText]}>{label}</Text>
    </View>
  );
}

const styles = StyleSheet.create({
  chip: {
    alignSelf: 'flex-start',
    backgroundColor: colors.chip,
    borderRadius: radius.sm,
    marginBottom: spacing.xs,
    marginRight: spacing.xs,
    paddingHorizontal: spacing.sm,
    paddingVertical: spacing.xs,
  },
  warning: {
    backgroundColor: '#fef3c7',
  },
  danger: {
    backgroundColor: '#fee2e2',
  },
  text: {
    color: colors.chipText,
    fontSize: 13,
    fontWeight: '700',
  },
  warningText: {
    color: '#92400e',
  },
  dangerText: {
    color: '#991b1b',
  },
});
