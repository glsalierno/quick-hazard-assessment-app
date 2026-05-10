import { NativeStackScreenProps } from '@react-navigation/native-stack';
import { useFocusEffect } from '@react-navigation/native';
import { useCallback, useState } from 'react';
import { FlatList, Pressable, StyleSheet, Text, View } from 'react-native';

import { Card } from '../components/Card';
import { PrimaryButton } from '../components/PrimaryButton';
import { EmptyText } from '../components/Rows';
import { Screen } from '../components/Screen';
import { colors, radius, spacing } from '../constants/theme';
import { ChemicalReport } from '../domain/types';
import { RootStackParamList } from '../navigation/types';
import { clearHistory, loadHistory } from '../storage/history';

type Props = NativeStackScreenProps<RootStackParamList, 'History'>;

export function HistoryScreen({ navigation }: Props) {
  const [reports, setReports] = useState<ChemicalReport[]>([]);

  const refresh = useCallback(async () => {
    setReports(await loadHistory());
  }, []);

  useFocusEffect(
    useCallback(() => {
      refresh();
    }, [refresh]),
  );

  const clear = async () => {
    await clearHistory();
    setReports([]);
  };

  return (
    <Screen>
      <View style={styles.header}>
        <Text style={styles.title}>Saved reports</Text>
        <Text style={styles.subtitle}>Reports are cached locally on this device for offline review.</Text>
      </View>

      <FlatList
        ListEmptyComponent={
          <Card>
            <EmptyText>No saved assessments yet. Run a chemical search to create local history.</EmptyText>
          </Card>
        }
        ListFooterComponent={
          reports.length > 0 ? (
            <View style={styles.footerActions}>
              <PrimaryButton onPress={clear} title="Clear saved reports" variant="danger" />
            </View>
          ) : null
        }
        data={reports}
        keyExtractor={(item) => item.id}
        renderItem={({ item }) => (
          <Pressable accessibilityRole="button" onPress={() => navigation.navigate('Results', { report: item })}>
            {({ pressed }) => (
              <View style={[styles.item, pressed && styles.pressed]}>
                <Text style={styles.itemTitle}>{item.dsstox?.preferredName ?? item.iupacName ?? item.normalizedQuery}</Text>
                <Text style={styles.itemMeta}>
                  {item.normalizedQuery} · CID {item.cid}
                </Text>
                <Text style={styles.itemDate}>{new Date(item.queriedAt).toLocaleString()}</Text>
              </View>
            )}
          </Pressable>
        )}
        showsVerticalScrollIndicator={false}
      />
    </Screen>
  );
}

const styles = StyleSheet.create({
  header: {
    paddingBottom: spacing.md,
    paddingTop: spacing.lg,
  },
  title: {
    color: colors.text,
    fontSize: 32,
    fontWeight: '900',
  },
  subtitle: {
    color: colors.muted,
    fontSize: 15,
    lineHeight: 22,
    marginTop: spacing.sm,
  },
  item: {
    backgroundColor: colors.surface,
    borderColor: colors.border,
    borderRadius: radius.lg,
    borderWidth: 1,
    marginBottom: spacing.md,
    padding: spacing.md,
  },
  pressed: {
    opacity: 0.75,
  },
  itemTitle: {
    color: colors.text,
    fontSize: 18,
    fontWeight: '800',
  },
  itemMeta: {
    color: colors.primaryDark,
    fontSize: 14,
    fontWeight: '700',
    marginTop: spacing.xs,
  },
  itemDate: {
    color: colors.muted,
    fontSize: 13,
    marginTop: spacing.xs,
  },
  footerActions: {
    marginBottom: spacing.xl,
  },
});
