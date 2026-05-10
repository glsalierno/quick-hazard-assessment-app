import { createNativeStackNavigator } from "@react-navigation/native-stack";

import { AssessmentScreen } from "../screens/AssessmentScreen";
import { HistoryScreen } from "../screens/HistoryScreen";
import { ResultsScreen } from "../screens/ResultsScreen";
import { StartScreen } from "../screens/StartScreen";
import { colors } from "../theme/colors";
import type { RootStackParamList } from "./types";

const Stack = createNativeStackNavigator<RootStackParamList>();

export function RootNavigator() {
  return (
    <Stack.Navigator
      screenOptions={{
        headerStyle: { backgroundColor: colors.primary },
        headerTintColor: "#ffffff",
        headerTitleStyle: { fontWeight: "800" },
        contentStyle: { backgroundColor: colors.background },
      }}
    >
      <Stack.Screen name="Start" component={StartScreen} options={{ title: "Quick Hazard Assessment" }} />
      <Stack.Screen name="Assessment" component={AssessmentScreen} options={{ title: "Assessing" }} />
      <Stack.Screen name="Results" component={ResultsScreen} options={{ title: "Assessment Results" }} />
      <Stack.Screen name="History" component={HistoryScreen} options={{ title: "History" }} />
      <Stack.Screen name="HistoryDetail" component={ResultsScreen} options={{ title: "Saved Assessment" }} />
    </Stack.Navigator>
  );
}
