import type { NativeStackScreenProps } from "@react-navigation/native-stack";
import { useEffect, useState } from "react";
import { ActivityIndicator, StyleSheet, Text, View } from "react-native";

import { Button } from "../components/Button";
import { Card } from "../components/Card";
import type { RootStackParamList } from "../navigation/types";
import { fetchChemicalAssessment } from "../services/pubchem";
import { saveAssessmentToHistory } from "../store/historyStore";
import { colors } from "../theme/colors";

type Props = NativeStackScreenProps<RootStackParamList, "Assessment">;

export function AssessmentScreen({ navigation, route }: Props) {
  const { query } = route.params;
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let isMounted = true;

    async function runAssessment() {
      setError(null);
      try {
        const assessment = await fetchChemicalAssessment(query);
        await saveAssessmentToHistory(assessment);
        if (isMounted) {
          navigation.replace("Results", { assessment });
        }
      } catch (assessmentError) {
        if (isMounted) {
          setError(assessmentError instanceof Error ? assessmentError.message : "Assessment failed.");
        }
      }
    }

    runAssessment();
    return () => {
      isMounted = false;
    };
  }, [navigation, query]);

  return (
    <View style={styles.container}>
      <Card>
        <ActivityIndicator color={colors.primary} size="large" />
        <Text style={styles.title}>Fetching hazard data</Text>
        <Text style={styles.body}>
          Looking up {query}, extracting GHS statements, physical hazards, toxicity endpoints, ecotoxicity, and acute exposure bands.
        </Text>
      </Card>

      {error ? (
        <Card>
          <Text style={styles.errorTitle}>Assessment error</Text>
          <Text style={styles.body}>{error}</Text>
          <View style={styles.actions}>
            <Button onPress={() => navigation.replace("Assessment", { query })}>Try again</Button>
            <Button variant="secondary" onPress={() => navigation.navigate("Start")}>
              Change query
            </Button>
          </View>
        </Card>
      ) : null}
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    justifyContent: "center",
    padding: 18,
  },
  title: {
    color: colors.text,
    fontSize: 22,
    fontWeight: "900",
    marginTop: 18,
    textAlign: "center",
  },
  body: {
    color: colors.muted,
    fontSize: 15,
    lineHeight: 22,
    marginTop: 8,
    textAlign: "center",
  },
  errorTitle: {
    color: colors.danger,
    fontSize: 20,
    fontWeight: "900",
    textAlign: "center",
  },
  actions: {
    gap: 10,
    marginTop: 18,
  },
});
