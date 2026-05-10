import { StyleSheet, Text, View } from 'react-native';

import { colors, spacing } from '../constants/theme';

type DetailRowProps = {
  label: string;
  value?: string | number | null;
};

export function DetailRow({ label, value }: DetailRowProps) {
  return (
    <View style={styles.row}>
      <Text style={styles.label}>{label}</Text>
      <Text selectable style={styles.value}>
        {value === undefined || value === null || value === '' ? '—' : value}
      </Text>
    </View>
  );
}

export function SectionTitle({ children }: { children: string }) {
  return <Text style={styles.sectionTitle}>{children}</Text>;
}

export function EmptyText({ children }: { children: string }) {
  return <Text style={styles.empty}>{children}</Text>;
}

const styles = StyleSheet.create({
  row: {
    borderBottomColor: colors.border,
    borderBottomWidth: StyleSheet.hairlineWidth,
    gap: spacing.xs,
    paddingVertical: spacing.sm,
  },
  label: {
    color: colors.muted,
    fontSize: 12,
    fontWeight: '700',
    textTransform: 'uppercase',
  },
  value: {
    color: colors.text,
    fontSize: 15,
    lineHeight: 21,
  },
  sectionTitle: {
    color: colors.text,
    fontSize: 19,
    fontWeight: '800',
    marginBottom: spacing.sm,
  },
  empty: {
    color: colors.muted,
    fontSize: 15,
    lineHeight: 21,
  },
});
