import { StatusBar } from 'expo-status-bar';
import { NavigationContainer } from '@react-navigation/native';
import { createNativeStackNavigator } from '@react-navigation/native-stack';
import { SafeAreaProvider } from 'react-native-safe-area-context';

import { colors } from './src/constants/theme';
import { RootStackParamList } from './src/navigation/types';
import { HistoryScreen } from './src/screens/HistoryScreen';
import { ResultsScreen } from './src/screens/ResultsScreen';
import { StartScreen } from './src/screens/StartScreen';

const Stack = createNativeStackNavigator<RootStackParamList>();

export default function App() {
  return (
    <SafeAreaProvider>
      <NavigationContainer>
        <StatusBar style="dark" />
        <Stack.Navigator
          screenOptions={{
            contentStyle: { backgroundColor: colors.background },
            headerShadowVisible: false,
            headerStyle: { backgroundColor: colors.background },
            headerTintColor: colors.primaryDark,
            headerTitleStyle: { color: colors.text, fontWeight: '800' },
          }}
        >
          <Stack.Screen component={StartScreen} name="Start" options={{ headerShown: false }} />
          <Stack.Screen component={ResultsScreen} name="Results" options={{ title: 'Assessment report' }} />
          <Stack.Screen component={HistoryScreen} name="History" options={{ title: 'History' }} />
        </Stack.Navigator>
      </NavigationContainer>
    </SafeAreaProvider>
  );
}
