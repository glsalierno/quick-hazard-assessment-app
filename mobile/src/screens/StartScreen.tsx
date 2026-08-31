import { NativeStackScreenProps } from '@react-navigation/native-stack';
import { useState } from 'react';
import {
  ActivityIndicator,
  KeyboardAvoidingView,
  Platform,
  Pressable,
  ScrollView,
  StyleSheet,
  Text,
  TextInput,
  View,
} from 'react-native';

import { Card } from '../components/Card';
import { Chip } from '../components/Chip';
import { PrimaryButton } from '../components/PrimaryButton';
import { Screen } from '../components/Screen';
import { getDioxolaneExampleReport } from '../constants/dioxolaneExample';
import { exampleChemicals } from '../constants/examples';
import { colors, radius, spacing } from '../constants/theme';
import { normalizeCasInput } from '../domain/cas';
import { RootStackParamList } from '../navigation/types';
import { assessCompound } from '../services/pubchem';
import { saveReportToHistory } from '../storage/history';

type Props = NativeStackScreenProps<RootStackParamList, 'Start'>;

export function StartScreen({ navigation }: Props) {
  const [query, setQuery] = useState('');
  const [error, setError] = useState<string | undefined>();
  const [loading, setLoading] = useState(false);

  const runAssessment = async (nextQuery = query) => {
    const normalized = normalizeCasInput(nextQuery);
    if (!normalized) {
      setError('Enter a CAS number or chemical name.');
      return;
    }

    setLoading(true);
    setError(undefined);
    try {
      const report = await assessCompound(normalized);
      await saveReportToHistory(report);
      navigation.navigate('Results', { report });
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : 'Unable to complete the PubChem lookup.');
    } finally {
      setLoading(false);
    }
  };

  return (
    <Screen>
      <KeyboardAvoidingView behavior={Platform.OS === 'ios' ? 'padding' : undefined} style={styles.flex}>
        <ScrollView keyboardShouldPersistTaps="handled" showsVerticalScrollIndicator={false}>
          <View style={styles.header}>
            <Text style={styles.emoji}>Hazard</Text>
            <Text style={styles.title}>Quick Hazard Assessment</Text>
            <Text style={styles.subtitle}>
              Search by CAS number or chemical name to build a mobile report from PubChem hazard, GHS, toxicity, and structure data.
            </Text>
          </View>

          <Card>
            <Text style={styles.examplesTitle}>Example report</Text>
            <Text style={styles.exampleCas}>646-06-0</Text>
            <Text style={styles.exampleReportTitle}>1,3-Dioxolane</Text>
            <Text style={styles.exampleHeadline}>Danger — highly flammable liquid; reproductive toxicity</Text>
            <Text style={styles.note}>
              Open a bundled PubChem snapshot with the generated hazard summary, GHS statements, and screening endpoints. No live lookup required.
            </Text>
            <View style={styles.exampleActions}>
              <PrimaryButton
                disabled={loading}
                onPress={() => {
                  const report = getDioxolaneExampleReport();
                  void saveReportToHistory(report);
                  navigation.navigate('Results', { report });
                }}
                title="Open example report"
              />
            </View>
          </Card>

          <Card>
            <Text style={styles.label}>CAS number or chemical name</Text>
            <TextInput
              autoCapitalize="none"
              autoCorrect={false}
              editable={!loading}
              onChangeText={setQuery}
              onSubmitEditing={() => runAssessment()}
              placeholder="67-64-1 or acetone"
              placeholderTextColor={colors.muted}
              returnKeyType="search"
              style={styles.input}
              value={query}
            />
            {error ? <Text style={styles.error}>{error}</Text> : null}
            <PrimaryButton disabled={loading} onPress={() => runAssessment()} title={loading ? 'Assessing...' : 'Assess chemical'} />
            {loading ? <ActivityIndicator color={colors.primary} style={styles.loader} /> : null}
          </Card>

          <Card>
            <Text style={styles.examplesTitle}>Examples from the Streamlit app</Text>
            <View style={styles.exampleGrid}>
              {exampleChemicals.map((item) => (
                <Pressable
                  accessibilityRole="button"
                  disabled={loading}
                  key={item.query}
                  onPress={() => {
                    setQuery(item.query);
                    runAssessment(item.query);
                  }}
                  style={({ pressed }) => [styles.exampleButton, pressed && styles.examplePressed]}
                >
                  <Text style={styles.exampleCas}>{item.query}</Text>
                  <Text style={styles.exampleLabel}>{item.label}</Text>
                </Pressable>
              ))}
            </View>
          </Card>

          <Card>
            <Text style={styles.examplesTitle}>Native mobile migration scope</Text>
            <View style={styles.chips}>
              <Chip label="React Native + Expo" />
              <Chip label="PubChem REST" />
              <Chip label="AsyncStorage history" />
              <Chip label="Offline cached reports" />
            </View>
            <Text style={styles.note}>
              The Python app has no question scoring tree; its assessment logic is API retrieval, extraction, prioritization, and report formatting.
            </Text>
          </Card>

          <PrimaryButton onPress={() => navigation.navigate('History')} title="View saved reports" variant="secondary" />
          <View style={styles.footer} />
        </ScrollView>
      </KeyboardAvoidingView>
    </Screen>
  );
}

const styles = StyleSheet.create({
  flex: {
    flex: 1,
  },
  header: {
    paddingBottom: spacing.md,
    paddingTop: spacing.lg,
  },
  emoji: {
    color: colors.primary,
    fontSize: 14,
    fontWeight: '800',
    letterSpacing: 2,
    textTransform: 'uppercase',
  },
  title: {
    color: colors.text,
    fontSize: 34,
    fontWeight: '900',
    letterSpacing: -0.8,
    marginTop: spacing.xs,
  },
  subtitle: {
    color: colors.muted,
    fontSize: 16,
    lineHeight: 23,
    marginTop: spacing.sm,
  },
  label: {
    color: colors.text,
    fontSize: 16,
    fontWeight: '700',
    marginBottom: spacing.sm,
  },
  input: {
    backgroundColor: colors.background,
    borderColor: colors.border,
    borderRadius: radius.md,
    borderWidth: 1,
    color: colors.text,
    fontSize: 17,
    marginBottom: spacing.md,
    paddingHorizontal: spacing.md,
    paddingVertical: 14,
  },
  error: {
    color: colors.danger,
    fontSize: 14,
    marginBottom: spacing.md,
  },
  loader: {
    marginTop: spacing.md,
  },
  examplesTitle: {
    color: colors.text,
    fontSize: 18,
    fontWeight: '800',
    marginBottom: spacing.md,
  },
  exampleGrid: {
    gap: spacing.sm,
  },
  exampleButton: {
    backgroundColor: colors.background,
    borderColor: colors.border,
    borderRadius: radius.md,
    borderWidth: 1,
    padding: spacing.md,
  },
  examplePressed: {
    opacity: 0.75,
  },
  exampleCas: {
    color: colors.primaryDark,
    fontSize: 16,
    fontWeight: '800',
  },
  exampleLabel: {
    color: colors.muted,
    marginTop: spacing.xs,
  },
  exampleReportTitle: {
    color: colors.text,
    fontSize: 24,
    fontWeight: '900',
    letterSpacing: -0.4,
    marginTop: spacing.xs,
  },
  exampleHeadline: {
    color: colors.danger,
    fontSize: 16,
    fontWeight: '700',
    lineHeight: 22,
    marginTop: spacing.sm,
  },
  exampleActions: {
    marginTop: spacing.md,
  },
  chips: {
    flexDirection: 'row',
    flexWrap: 'wrap',
  },
  note: {
    color: colors.muted,
    fontSize: 15,
    lineHeight: 22,
    marginTop: spacing.sm,
  },
  footer: {
    height: spacing.xl,
  },
});
