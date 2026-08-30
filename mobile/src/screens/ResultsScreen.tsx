import { NativeStackScreenProps } from '@react-navigation/native-stack';
import { Image, ScrollView, StyleSheet, Text, View } from 'react-native';

import { Card } from '../components/Card';
import { Chip } from '../components/Chip';
import { DetailRow, EmptyText, SectionTitle } from '../components/Rows';
import { Screen } from '../components/Screen';
import { researchCitation } from '../constants/examples';
import { colors, radius, spacing } from '../constants/theme';
import { phrasePairs } from '../domain/ghs';
import { summarizeReport } from '../domain/hazardSummary';
import { PrioritizedToxicityItem } from '../domain/types';
import { RootStackParamList } from '../navigation/types';

type Props = NativeStackScreenProps<RootStackParamList, 'Results'>;

function joinValues(values: string[]): string {
  return values.length > 0 ? values.join(' | ') : '—';
}

function exposureLabel(label: string, band?: { value: number; metric: string; band: number }) {
  if (!band) {
    return null;
  }
  const unit = band.metric === 'ld50_mg_kg' ? 'mg/kg' : 'mg/m3';
  return <DetailRow label={label} value={`${band.value} ${unit}; GHS-style band ${band.band}`} />;
}

function ToxicityItem({ item }: { item: PrioritizedToxicityItem }) {
  return (
    <View style={styles.toxicityItem}>
      <View style={styles.toxicityHeader}>
        <Chip label={item.bucket} tone={item.bucket === 'Quantitative' ? 'info' : 'warning'} />
        <Chip label={item.source} />
      </View>
      <Text style={styles.toxEndpoint}>{item.endpoint}</Text>
      <Text style={styles.toxValue}>{item.value}</Text>
      <Text style={styles.toxMeta}>
        {item.route || 'Route unknown'} · {item.species || 'Species unknown'} {item.units ? `· ${item.units}` : ''}
      </Text>
    </View>
  );
}

export function ResultsScreen({ route }: Props) {
  const { report } = route.params;
  const hazardSummary = report.hazardSummary ?? summarizeReport(report);
  const hPhrases = phrasePairs(report.ghs.hCodes, 'h');
  const pPhrases = phrasePairs(report.ghs.pCodes, 'p');
  const hasAnyGhs = hPhrases.length > 0 || pPhrases.length > 0 || Boolean(report.ghs.signalWord);
  const topToxicity = report.prioritizedToxicity.slice(0, 12);

  return (
    <Screen>
      <ScrollView showsVerticalScrollIndicator={false}>
        <View style={styles.header}>
          <Text style={styles.kicker}>{report.inputType === 'cas' ? 'CAS assessment' : 'Name assessment'}</Text>
          <Text style={styles.title}>{report.dsstox?.preferredName ?? report.iupacName ?? report.normalizedQuery}</Text>
          <Text style={styles.subtitle}>PubChem CID {report.cid} · saved {new Date(report.queriedAt).toLocaleString()}</Text>
        </View>

        <Card>
          <SectionTitle>Hazard summary</SectionTitle>
          <View style={styles.summaryHeader}>
            <Chip
              label={hazardSummary.concernLevel === 'high' ? 'High concern' : `${hazardSummary.concernLevel} concern`}
              tone={hazardSummary.concernLevel === 'high' ? 'danger' : hazardSummary.concernLevel === 'moderate' ? 'warning' : 'info'}
            />
          </View>
          <Text style={styles.summaryHeadline}>{hazardSummary.headline}</Text>
          {hazardSummary.paragraphs.map((paragraph) => (
            <Text key={paragraph} style={styles.summaryParagraph}>
              {paragraph}
            </Text>
          ))}
          <View style={styles.chips}>
            {hazardSummary.highlights.map((item) => (
              <Chip key={`${item.label}-${item.value}`} label={`${item.label}: ${item.value}`} tone={item.tone} />
            ))}
          </View>
          <Text style={styles.summaryNote}>
            Lookup synthesis from PubChem GHS and property text. Use the SDS and local controls for work planning.
          </Text>
        </Card>

        <Card>
          <SectionTitle>Molecular structure</SectionTitle>
          <Image resizeMode="contain" source={{ uri: report.structureImageUrl }} style={styles.structure} />
          <DetailRow label="SMILES" value={report.smiles} />
        </Card>

        <Card>
          <SectionTitle>Identifiers</SectionTitle>
          <DetailRow label="Query" value={report.normalizedQuery} />
          <DetailRow label="IUPAC name" value={report.iupacName} />
          <DetailRow label="Molecular formula" value={report.formula} />
          <DetailRow label="Molecular weight" value={report.molecularWeight ? `${report.molecularWeight} g/mol` : undefined} />
          <DetailRow label="DTXSID" value={report.dsstox?.dtxsid ?? 'Not in bundled mobile sample'} />
        </Card>

        <Card>
          <SectionTitle>Key properties</SectionTitle>
          <DetailRow label="Flash point" value={joinValues(report.flashPoint)} />
          <DetailRow label="Vapor pressure" value={joinValues(report.vaporPressure)} />
          <DetailRow label="NFPA" value={report.nfpa} />
          <DetailRow label="IARC" value={report.iarc} />
          <DetailRow label="Prop 65" value={report.prop65} />
        </Card>

        <Card>
          <SectionTitle>GHS classification</SectionTitle>
          {hasAnyGhs ? (
            <>
              <DetailRow label="Signal word" value={report.ghs.signalWord} />
              <Text style={styles.subsection}>Hazard statements</Text>
              {hPhrases.length > 0 ? (
                hPhrases.map((item) => <DetailRow key={item.code} label={item.code} value={item.phrase} />)
              ) : (
                <EmptyText>No hazard statements found.</EmptyText>
              )}
              <Text style={styles.subsection}>Precautionary statements</Text>
              {pPhrases.length > 0 ? (
                pPhrases.map((item) => <DetailRow key={item.code} label={item.code} value={item.phrase} />)
              ) : (
                <EmptyText>No precautionary statements found.</EmptyText>
              )}
            </>
          ) : (
            <EmptyText>No GHS classification data available from PubChem.</EmptyText>
          )}
        </Card>

        <Card>
          <SectionTitle>Toxic doses and endpoints</SectionTitle>
          {topToxicity.length > 0 ? (
            topToxicity.map((item, index) => <ToxicityItem item={item} key={`${item.endpoint}-${index}`} />)
          ) : (
            <EmptyText>No toxicity endpoints found in current PubChem data.</EmptyText>
          )}
          {report.prioritizedToxicity.length > topToxicity.length ? (
            <Text style={styles.moreText}>Showing first {topToxicity.length} of {report.prioritizedToxicity.length} endpoints.</Text>
          ) : null}
        </Card>

        <Card>
          <SectionTitle>Ecotoxicity</SectionTitle>
          <DetailRow label="Aquatic GHS codes" value={joinValues(report.ecotoxicity.hCodesAquatic)} />
          <DetailRow label="Aquatic LC50" value={report.ecotoxicity.aquaticLc50MgL ? `${report.ecotoxicity.aquaticLc50MgL} mg/L` : undefined} />
          <DetailRow label="Aquatic EC50" value={report.ecotoxicity.aquaticEc50MgL ? `${report.ecotoxicity.aquaticEc50MgL} mg/L` : undefined} />
          {report.ecotoxicity.entries.slice(0, 5).map((entry, index) => (
            <View key={`${entry.value}-${index}`} style={styles.ecoEntry}>
              <Text style={styles.toxEndpoint}>{entry.endpoint ?? 'Aquatic toxicity'} · {entry.species}</Text>
              <Text style={styles.toxValue}>{entry.valueNum ?? entry.value} {entry.unit}</Text>
              {entry.duration ? <Text style={styles.toxMeta}>Duration: {entry.duration}</Text> : null}
            </View>
          ))}
          {report.ecotoxicity.entries.length === 0 ? <EmptyText>No aquatic toxicity endpoints parsed.</EmptyText> : null}
        </Card>

        <Card>
          <SectionTitle>Exposure bands</SectionTitle>
          {exposureLabel('Oral', report.exposureBands.oral)}
          {exposureLabel('Dermal', report.exposureBands.dermal)}
          {exposureLabel('Inhalation', report.exposureBands.inhalation)}
          {!report.exposureBands.oral && !report.exposureBands.dermal && !report.exposureBands.inhalation ? (
            <EmptyText>No LD50/LC50 exposure bands could be computed from PubChem text.</EmptyText>
          ) : null}
        </Card>

        <Text style={styles.citation}>For research use, cite Zenodo DOI {researchCitation}. Data sources: PubChem and bundled DSSTox examples.</Text>
        <View style={styles.footer} />
      </ScrollView>
    </Screen>
  );
}

const styles = StyleSheet.create({
  header: {
    paddingBottom: spacing.md,
    paddingTop: spacing.lg,
  },
  kicker: {
    color: colors.primary,
    fontSize: 13,
    fontWeight: '800',
    letterSpacing: 1.5,
    textTransform: 'uppercase',
  },
  title: {
    color: colors.text,
    fontSize: 30,
    fontWeight: '900',
    letterSpacing: -0.6,
    marginTop: spacing.xs,
  },
  subtitle: {
    color: colors.muted,
    fontSize: 14,
    marginTop: spacing.sm,
  },
  summaryHeader: {
    flexDirection: 'row',
    flexWrap: 'wrap',
    marginBottom: spacing.sm,
  },
  summaryHeadline: {
    color: colors.text,
    fontSize: 20,
    fontWeight: '800',
    letterSpacing: -0.3,
    lineHeight: 26,
  },
  summaryParagraph: {
    color: colors.text,
    fontSize: 15,
    lineHeight: 22,
    marginTop: spacing.sm,
  },
  summaryNote: {
    color: colors.muted,
    fontSize: 13,
    lineHeight: 19,
    marginTop: spacing.md,
  },
  chips: {
    flexDirection: 'row',
    flexWrap: 'wrap',
    marginTop: spacing.md,
  },
  structure: {
    alignSelf: 'center',
    backgroundColor: colors.background,
    borderRadius: radius.md,
    height: 220,
    marginBottom: spacing.md,
    width: '100%',
  },
  subsection: {
    color: colors.text,
    fontSize: 16,
    fontWeight: '800',
    marginTop: spacing.md,
  },
  toxicityItem: {
    backgroundColor: colors.background,
    borderRadius: radius.md,
    marginBottom: spacing.sm,
    padding: spacing.md,
  },
  toxicityHeader: {
    flexDirection: 'row',
    flexWrap: 'wrap',
  },
  toxEndpoint: {
    color: colors.text,
    fontSize: 16,
    fontWeight: '800',
    marginTop: spacing.xs,
  },
  toxValue: {
    color: colors.text,
    fontSize: 14,
    lineHeight: 20,
    marginTop: spacing.xs,
  },
  toxMeta: {
    color: colors.muted,
    fontSize: 13,
    marginTop: spacing.xs,
  },
  moreText: {
    color: colors.muted,
    fontSize: 13,
    marginTop: spacing.sm,
  },
  ecoEntry: {
    borderTopColor: colors.border,
    borderTopWidth: StyleSheet.hairlineWidth,
    paddingVertical: spacing.sm,
  },
  citation: {
    color: colors.muted,
    fontSize: 13,
    lineHeight: 19,
    paddingHorizontal: spacing.sm,
    textAlign: 'center',
  },
  footer: {
    height: spacing.xl,
  },
});
