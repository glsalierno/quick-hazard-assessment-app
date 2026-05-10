import { StyleSheet, Text, View } from "react-native";

import { colors } from "../theme/colors";

type BadgeProps = {
  label: string;
  tone?: "danger" | "warning" | "info";
};

export function Badge({ label, tone = "info" }: BadgeProps) {
  return (
    <View style={[styles.badge, tone === "danger" && styles.danger, tone === "warning" && styles.warning]}>
      <Text style={[styles.label, tone === "danger" && styles.dangerLabel, tone === "warning" && styles.warningLabel]}>{label}</Text>
    </View>
  );
}

const styles = StyleSheet.create({
  badge: {
    alignSelf: "flex-start",
    backgroundColor: "#dbeafe",
    borderRadius: 999,
    marginBottom: 8,
    marginRight: 8,
    paddingHorizontal: 10,
    paddingVertical: 6,
  },
  danger: {
    backgroundColor: "#fee2e2",
  },
  warning: {
    backgroundColor: "#fef3c7",
  },
  label: {
    color: colors.info,
    fontSize: 13,
    fontWeight: "700",
  },
  dangerLabel: {
    color: colors.danger,
  },
  warningLabel: {
    color: colors.warning,
  },
});
