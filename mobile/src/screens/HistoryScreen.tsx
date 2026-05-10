import { useFocusEffect } from "@react-navigation/native";
import type { NativeStackScreenProps } from "@react-navigation/native-stack";
import { useCallback, useState } from "react";
import { Alert, Pressable, ScrollView, StyleSheet, Text, View } from "react-native";

import { Button } from "../components/Button";
import { Card } from "../components/Card";
import type { RootStackParamList } from "../navigation/types";
import { clearHistory, getHistory } from "../store/historyStore";
import { colors } from "../theme/colors";
import type { AssessmentHistoryItem } from "../types/chemical";

type Props = NativeStackScreenProps<RootStackParamList, "History">;

export function HistoryScreen({ navigation }: Props) {
  const [history, setHistory] = useState<AssessmentHistoryItem[]>([]);

  const loadHistory = useCallback(async () => {
    setHistory(await getHistory());
  }, []);

  useFocusEffect(
    useCallback(() => {
      loadHistory();
    }, [loadHistory]),
  );

  async function confirmClearHistory() {
    Alert.alert("Clear history?", "This removes saved assessments from this device.", [
      { text: "Cancel", style: "cancel" },
      {
        text: "Clear",
        style: "destructive",
        onPress: async () => {
          await clearHistory();
          setHistory([]);
        },
      },
    ]);
  }

  return (
    <ScrollView contentContainerStyle={styles.container}>
      {history.length === 0 ? (
        <Card>
          <Text style={styles.title}>No saved assessments yet</Text>
          <Text style={styles.body}>Run a chemical assessment and it will be stored locally on this device for quick review.</Text>
          <View style={styles.action}>
            <Button onPress={() => navigation.navigate("Start")}>Start assessment</Button>
          </View>
        </Card>
      ) : (
        <>
          {history.map((item) => (
            <Pressable key={item.id} onPress={() => navigation.navigate("HistoryDetail", { item })}>
              <Card>
                <Text style={styles.title}>{item.title}</Text>
                <Text style={styles.body}>
                  {item.query}
                  {item.subtitle ? ` · ${item.subtitle}` : ""}
                </Text>
                <Text style={styles.date}>{new Date(item.assessedAt).toLocaleString()}</Text>
              </Card>
            </Pressable>
          ))}
          <Button variant="danger" onPress={confirmClearHistory}>
            Clear local history
          </Button>
        </>
      )}
    </ScrollView>
  );
}

const styles = StyleSheet.create({
  container: {
    padding: 18,
    paddingBottom: 42,
  },
  title: {
    color: colors.text,
    fontSize: 20,
    fontWeight: "900",
  },
  body: {
    color: colors.muted,
    fontSize: 15,
    lineHeight: 22,
    marginTop: 5,
  },
  date: {
    color: colors.primary,
    fontSize: 13,
    fontWeight: "800",
    marginTop: 10,
  },
  action: {
    marginTop: 18,
  },
});
