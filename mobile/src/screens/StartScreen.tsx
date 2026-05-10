import type { NativeStackScreenProps } from "@react-navigation/native-stack";
import { useState } from "react";
import { KeyboardAvoidingView, Platform, ScrollView, StyleSheet, Text, TextInput, View } from "react-native";

import { Button } from "../components/Button";
import { Card } from "../components/Card";
import { APP_TITLE, EXAMPLE_CHEMICALS } from "../data/examples";
import { colors } from "../theme/colors";
import { normalizeCasInput } from "../utils/cas";
import type { RootStackParamList } from "../navigation/types";

type Props = NativeStackScreenProps<RootStackParamList, "Start">;

export function StartScreen({ navigation }: Props) {
  const [query, setQuery] = useState("");
  const normalized = normalizeCasInput(query);
  const canSubmit = normalized.length > 0;

  function submit(value = query) {
    const nextQuery = normalizeCasInput(value);
    if (!nextQuery) {
      return;
    }
    navigation.navigate("Assessment", { query: nextQuery });
  }

  return (
    <KeyboardAvoidingView behavior={Platform.select({ ios: "padding", android: undefined })} style={styles.flex}>
      <ScrollView contentContainerStyle={styles.container} keyboardShouldPersistTaps="handled">
        <View style={styles.hero}>
          <Text style={styles.emoji}>🧪</Text>
          <Text style={styles.title}>{APP_TITLE}</Text>
          <Text style={styles.subtitle}>
            Native mobile assessment for CAS numbers and chemical names using PubChem hazard, GHS, toxicity, and ecotoxicity data.
          </Text>
        </View>

        <Card>
          <Text style={styles.label}>CAS number or chemical name</Text>
          <TextInput
            autoCapitalize="none"
            autoCorrect={false}
            clearButtonMode="while-editing"
            onChangeText={setQuery}
            onSubmitEditing={() => submit()}
            placeholder="e.g., 67-64-1 or acetone"
            returnKeyType="search"
            style={styles.input}
            value={query}
          />
          <Button disabled={!canSubmit} onPress={() => submit()}>
            Assess chemical
          </Button>
        </Card>

        <Card>
          <Text style={styles.sectionTitle}>Examples</Text>
          <View style={styles.exampleGrid}>
            {EXAMPLE_CHEMICALS.map((example) => (
              <View key={example.query} style={styles.exampleButton}>
                <Button variant="secondary" onPress={() => submit(example.query)}>
                  {example.label}
                </Button>
              </View>
            ))}
          </View>
        </Card>

        <Card>
          <Text style={styles.sectionTitle}>Mobile data model</Text>
          <Text style={styles.body}>
            This first native version stores assessment history on-device and calls PubChem directly. DSSTox/ToxValDB are modeled for migration, but the bundled
            Streamlit SQLite database should be moved behind a small API or packaged with Expo SQLite in a follow-up phase.
          </Text>
        </Card>

        <Button variant="secondary" onPress={() => navigation.navigate("History")}>
          View local history
        </Button>
      </ScrollView>
    </KeyboardAvoidingView>
  );
}

const styles = StyleSheet.create({
  flex: {
    flex: 1,
  },
  container: {
    padding: 18,
    paddingBottom: 40,
  },
  hero: {
    marginBottom: 18,
  },
  emoji: {
    fontSize: 44,
  },
  title: {
    color: colors.text,
    fontSize: 32,
    fontWeight: "900",
    letterSpacing: -0.5,
  },
  subtitle: {
    color: colors.muted,
    fontSize: 16,
    lineHeight: 23,
    marginTop: 8,
  },
  label: {
    color: colors.text,
    fontSize: 15,
    fontWeight: "800",
    marginBottom: 8,
  },
  input: {
    backgroundColor: "#f1f5f9",
    borderColor: colors.border,
    borderRadius: 14,
    borderWidth: 1,
    color: colors.text,
    fontSize: 18,
    marginBottom: 14,
    paddingHorizontal: 14,
    paddingVertical: 13,
  },
  sectionTitle: {
    color: colors.text,
    fontSize: 18,
    fontWeight: "900",
    marginBottom: 10,
  },
  exampleGrid: {
    gap: 10,
  },
  exampleButton: {
    width: "100%",
  },
  body: {
    color: colors.muted,
    fontSize: 15,
    lineHeight: 22,
  },
});
