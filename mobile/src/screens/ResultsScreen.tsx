import type { NativeStackScreenProps } from "@react-navigation/native-stack";
import { ScrollView, StyleSheet, Text, View } from "react-native";

import { Badge } from "../components/Badge";
import { Button } from "../components/Button";
import { Card } from "../components/Card";
import { getHPhrase, getPPhrase } from "../data/ghsPhrases";
import type { RootStackParamList } from "../navigation/types";
import { buildReportRows, getOverallHazardBadges, prioritizeToxicityData, sortToxicityForDisplay } from "../services/assessment";
import { colors } from "../theme/colors";
import type { ChemicalAssessment } from "../types/chemical";

type ResultsProps =
  | NativeStackScreenProps<RootStackParamList, "Results">
  | NativeStackScreenProps<RootStackParamList, "HistoryDetail">;

function getAssessment(route: ResultsProps["route"]): ChemicalAssessment {
  if (route.name === "HistoryDetail") {
    return route.params.item.assessment;
  }
  return route.params.assessment;
}

export function ResultsScreen({ navigation, route }: ResultsProps) {
  const assessment = getAssessment(route);
  const prioritized = prioritizeToxicityData(assessment);

  return (
    <ScrollView contentContainerStyle={styles.container}>
      <Card>
        <Text style={styles.kicker}>Assessment complete</Text>
        <Text style={styles.title}>{assessment.dsstox?.preferredName ?? assessment.iupacName ?? assessment.normalizedQuery}</Text>
        <Text style={styles.subtitle}>
          {assessment.formula ?? "Formula unavailable"} · CID {assessment.cid}
        </Text>
        <View style={styles.badgeRow}>
          {getOverallHazardBadges(assessment).map((badge) => (
            <Badge key={badge.label} label={badge.label} tone={badge.tone} />
          ))}
        </View>
      </Card>

      <Card>
        <Text style={styles.sectionTitle}>Identifiers and key properties</Text>
        {buildReportRows(assessment).map((row) => (
          <View key={row.label} style={styles.row}>
            <Text style={styles.rowLabel}>{row.label}</Text>
            <Text style={styles.rowValue}>{row.value}</Text>
          </View>
        ))}
        {assessment.smiles ? (
          <View style={styles.smilesBox}>
            <Text style={styles.rowLabel}>SMILES</Text>
            <Text selectable style={styles.mono}>
              {assessment.smiles}
            </Text>
          </View>
        ) : null}
      </Card>

      <Card>
        <Text style={styles.sectionTitle}>GHS classification</Text>
        {assessment.ghs.signalWord ? <Text style={styles.signal}>Signal word: {assessment.ghs.signalWord}</Text> : null}
        <Text style={styles.subhead}>Hazard statements</Text>
        {assessment.ghs.hCodes.length > 0 ? (
          assessment.ghs.hCodes.map((code) => (
            <Text key={code} style={styles.statement}>
              <Text style={styles.code}>{code}: </Text>
              {getHPhrase(code)}
            </Text>
          ))
        ) : (
          <Text style={styles.empty}>No hazard statements found.</Text>
        )}
        <Text style={styles.subhead}>Precautionary statements</Text>
        {assessment.ghs.pCodes.length > 0 ? (
          assessment.ghs.pCodes.map((code) => (
            <Text key={code} style={styles.statement}>
              <Text style={styles.code}>{code}: </Text>
              {getPPhrase(code)}
            </Text>
          ))
        ) : (
          <Text style={styles.empty}>No precautionary statements found.</Text>
        )}
      </Card>

      <Card>
        <Text style={styles.sectionTitle}>Toxic doses and endpoints</Text>
        <Text style={styles.caption}>
          {prioritized.quantitative.length} quantitative and {prioritized.categorical.length} categorical endpoints found.
        </Text>
        {sortToxicityForDisplay(assessment.toxicities)
          .slice(0, 12)
          .map((toxicity, index) => (
            <View key={`${toxicity.type}-${index}`} style={styles.toxicityItem}>
              <Text style={styles.toxicityTitle}>
                {toxicity.type} · {toxicity.route}
              </Text>
              <Text style={styles.toxicityMeta}>
                {toxicity.species}
                {toxicity.unit ? ` · ${toxicity.unit}` : ""}
              </Text>
              <Text style={styles.toxicityValue}>{toxicity.value}</Text>
            </View>
          ))}
        {assessment.toxicities.length === 0 ? <Text style={styles.empty}>No toxicity endpoints found in PubChem.</Text> : null}
      </Card>

      <Card>
        <Text style={styles.sectionTitle}>Ecotoxicity</Text>
        {assessment.ecotoxicity.hCodesAquatic.length > 0 ? (
          <Text style={styles.statement}>Aquatic GHS: {assessment.ecotoxicity.hCodesAquatic.join(", ")}</Text>
        ) : null}
        {assessment.ecotoxicity.aquaticLc50MgL != null ? (
          <Text style={styles.statement}>LC50: {assessment.ecotoxicity.aquaticLc50MgL} mg/L</Text>
        ) : null}
        {assessment.ecotoxicity.aquaticEc50MgL != null ? (
          <Text style={styles.statement}>EC50: {assessment.ecotoxicity.aquaticEc50MgL} mg/L</Text>
        ) : null}
        {assessment.ecotoxicity.entries.length === 0 ? <Text style={styles.empty}>No aquatic endpoints found.</Text> : null}
      </Card>

      <View style={styles.actions}>
        <Button onPress={() => navigation.navigate("Start")}>Assess another chemical</Button>
        <Button variant="secondary" onPress={() => navigation.navigate("History")}>
          View history
        </Button>
      </View>
    </ScrollView>
  );
}

const styles = StyleSheet.create({
  container: {
    padding: 18,
    paddingBottom: 42,
  },
  kicker: {
    color: colors.primary,
    fontSize: 13,
    fontWeight: "900",
    letterSpacing: 0.8,
    textTransform: "uppercase",
  },
  title: {
    color: colors.text,
    fontSize: 26,
    fontWeight: "900",
    marginTop: 4,
  },
  subtitle: {
    color: colors.muted,
    fontSize: 15,
    marginTop: 4,
  },
  badgeRow: {
    flexDirection: "row",
    flexWrap: "wrap",
    marginTop: 14,
  },
  sectionTitle: {
    color: colors.text,
    fontSize: 19,
    fontWeight: "900",
    marginBottom: 12,
  },
  row: {
    borderBottomColor: colors.border,
    borderBottomWidth: 1,
    paddingVertical: 9,
  },
  rowLabel: {
    color: colors.muted,
    fontSize: 13,
    fontWeight: "800",
  },
  rowValue: {
    color: colors.text,
    fontSize: 16,
    marginTop: 2,
  },
  smilesBox: {
    backgroundColor: "#f1f5f9",
    borderRadius: 12,
    marginTop: 12,
    padding: 12,
  },
  mono: {
    color: colors.text,
    fontFamily: "monospace",
    fontSize: 13,
    marginTop: 5,
  },
  signal: {
    color: colors.danger,
    fontSize: 16,
    fontWeight: "900",
    marginBottom: 10,
  },
  subhead: {
    color: colors.text,
    fontSize: 16,
    fontWeight: "900",
    marginTop: 10,
  },
  statement: {
    color: colors.text,
    fontSize: 15,
    lineHeight: 22,
    marginTop: 7,
  },
  code: {
    fontWeight: "900",
  },
  empty: {
    color: colors.muted,
    fontSize: 15,
    marginTop: 8,
  },
  caption: {
    color: colors.muted,
    fontSize: 14,
    marginBottom: 10,
  },
  toxicityItem: {
    borderTopColor: colors.border,
    borderTopWidth: 1,
    paddingVertical: 12,
  },
  toxicityTitle: {
    color: colors.text,
    fontSize: 15,
    fontWeight: "900",
  },
  toxicityMeta: {
    color: colors.muted,
    fontSize: 13,
    marginTop: 2,
  },
  toxicityValue: {
    color: colors.text,
    fontSize: 14,
    lineHeight: 21,
    marginTop: 6,
  },
  actions: {
    gap: 10,
  },
});
