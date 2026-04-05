#include "benchmark/workload_dsl.hpp"

#include <algorithm>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <cmath>
#include <cstdlib>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace schedlab::benchmark::dsl {

namespace {

struct ParsedLine {
  std::size_t line = 0;
  std::size_t indent = 0;
  bool opens_block = false;
  std::vector<std::string> tokens;
};

struct CallReference {
  std::size_t line = 0;
  std::string device;
};

struct NamedDeclaration {
  std::size_t line = 0;
  std::string name;
};

struct NodeDeclaration {
  std::size_t line = 0;
  std::string name;
  uint64_t worker_begin = 0;
  uint64_t worker_end = 0;
};

struct UseReference {
  std::size_t line = 0;
  std::string template_name;
  std::size_t arg_count = 0;
};

struct TemplateDeclaration {
  std::size_t line = 0;
  std::string name;
  std::size_t param_count = 0;
};

struct VariantOverrideReference {
  std::size_t line = 0;
  std::string variant_name;
  std::string param_name;
};

struct ParamReference {
  std::size_t line = 0;
  std::string name;
  std::string context;
  std::vector<std::string> template_params;
};

std::string trim_right(std::string input) {
  while (!input.empty() && std::isspace(static_cast<unsigned char>(input.back())) != 0) {
    input.pop_back();
  }
  return input;
}

std::vector<std::string> split_tokens(std::string_view text) {
  std::vector<std::string> tokens;
  std::istringstream stream{std::string(text)};
  std::string token;
  while (stream >> token) {
    tokens.push_back(token);
  }
  return tokens;
}

bool is_identifier_token(std::string_view token) {
  if (token.empty()) {
    return false;
  }
  const auto is_initial = [](char ch) {
    return std::isalpha(static_cast<unsigned char>(ch)) != 0 || ch == '_';
  };
  const auto is_continue = [](char ch) {
    return std::isalnum(static_cast<unsigned char>(ch)) != 0 || ch == '_';
  };
  if (!is_initial(token.front())) {
    return false;
  }
  for (std::size_t i = 1; i < token.size(); ++i) {
    if (!is_continue(token[i])) {
      return false;
    }
  }
  return true;
}

std::optional<std::vector<std::string>> split_csv_tokens(std::string_view csv) {
  std::vector<std::string> parts;
  if (csv.empty()) {
    return parts;
  }
  std::size_t start = 0;
  while (start <= csv.size()) {
    const std::size_t comma = csv.find(',', start);
    const std::size_t end = (comma == std::string_view::npos) ? csv.size() : comma;
    const std::string_view piece = csv.substr(start, end - start);
    if (piece.empty()) {
      return std::nullopt;
    }
    parts.emplace_back(piece);
    if (comma == std::string_view::npos) {
      break;
    }
    start = comma + 1;
  }
  return parts;
}

bool parse_call_like_token(std::string_view token, std::string* name,
                           std::vector<std::string>* args) {
  if (token.empty()) {
    return false;
  }
  const std::size_t open = token.find('(');
  if (open == std::string_view::npos || token.back() != ')') {
    return false;
  }
  const std::string_view name_view = token.substr(0, open);
  if (!is_identifier_token(name_view)) {
    return false;
  }
  *name = std::string(name_view);
  const std::string_view args_view = token.substr(open + 1, token.size() - open - 2);
  const auto parsed_args = split_csv_tokens(args_view);
  if (!parsed_args.has_value()) {
    return false;
  }
  *args = *parsed_args;
  return true;
}

std::string concat_tokens_without_spaces(const std::vector<std::string>& tokens,
                                         std::size_t start) {
  std::string merged;
  for (std::size_t i = start; i < tokens.size(); ++i) {
    merged.append(tokens[i]);
  }
  return merged;
}

std::optional<std::pair<std::string, std::size_t>>
collect_balanced_token_sequence(const std::vector<std::string>& tokens, std::size_t start) {
  if (start >= tokens.size()) {
    return std::nullopt;
  }
  std::string merged;
  int paren_balance = 0;
  bool saw_paren = false;
  std::size_t index = start;
  for (; index < tokens.size(); ++index) {
    const std::string& token = tokens[index];
    merged.append(token);
    for (const char ch : token) {
      if (ch == '(') {
        ++paren_balance;
        saw_paren = true;
      } else if (ch == ')') {
        --paren_balance;
      }
    }
    if (paren_balance < 0) {
      return std::nullopt;
    }
    if (saw_paren && paren_balance == 0) {
      return std::pair<std::string, std::size_t>{merged, index + 1};
    }
  }
  return std::nullopt;
}

std::optional<uint64_t> parse_u64(std::string_view token) {
  if (token.empty()) {
    return std::nullopt;
  }
  uint64_t value = 0;
  for (const char ch : token) {
    if (!std::isdigit(static_cast<unsigned char>(ch))) {
      return std::nullopt;
    }
    value = value * 10 + static_cast<uint64_t>(ch - '0');
  }
  return value;
}

std::optional<std::pair<uint64_t, uint64_t>> parse_worker_range(std::string_view token) {
  const std::size_t dash = token.find('-');
  if (dash == std::string_view::npos) {
    return std::nullopt;
  }
  const auto begin = parse_u64(token.substr(0, dash));
  const auto end = parse_u64(token.substr(dash + 1));
  if (!begin.has_value() || !end.has_value() || *begin > *end) {
    return std::nullopt;
  }
  return std::pair<uint64_t, uint64_t>{*begin, *end};
}

std::optional<double> parse_f64(std::string_view token) {
  if (token.empty()) {
    return std::nullopt;
  }
  char* end = nullptr;
  const std::string text(token);
  const double value = std::strtod(text.c_str(), &end);
  if (end == text.c_str() || *end != '\0' || !std::isfinite(value)) {
    return std::nullopt;
  }
  return value;
}

std::optional<Duration> parse_duration(std::string_view token) {
  if (token.size() < 2) {
    return std::nullopt;
  }

  std::size_t split = 0;
  while (split < token.size() && std::isdigit(static_cast<unsigned char>(token[split])) != 0) {
    ++split;
  }
  if (split == 0 || split == token.size()) {
    return std::nullopt;
  }

  const auto value = parse_u64(token.substr(0, split));
  if (!value.has_value()) {
    return std::nullopt;
  }

  const std::string_view suffix = token.substr(split);
  DurationUnit unit = DurationUnit::Microseconds;
  if (suffix == "us") {
    unit = DurationUnit::Microseconds;
  } else if (suffix == "ms") {
    unit = DurationUnit::Milliseconds;
  } else if (suffix == "s") {
    unit = DurationUnit::Seconds;
  } else {
    return std::nullopt;
  }

  return Duration{
      .value = *value,
      .unit = unit,
  };
}

std::optional<Track> parse_track(std::string_view token) {
  if (token == "cpu_bound") {
    return Track::CpuBound;
  }
  if (token == "io_bound") {
    return Track::IoBound;
  }
  if (token == "mixed") {
    return Track::Mixed;
  }
  if (token == "throughput") {
    return Track::Throughput;
  }
  if (token == "latency") {
    return Track::Latency;
  }
  if (token == "fairness") {
    return Track::Fairness;
  }
  return std::nullopt;
}

std::optional<ScorerKind> parse_scorer_kind(std::string_view token) {
  if (token == "throughput_makespan") {
    return ScorerKind::ThroughputMakespan;
  }
  if (token == "throughput_sustained_rate") {
    return ScorerKind::ThroughputSustainedRate;
  }
  if (token == "latency_wakeup_p99") {
    return ScorerKind::LatencyWakeupP99;
  }
  if (token == "latency_flow_p99") {
    return ScorerKind::LatencyFlowP99;
  }
  if (token == "fairness_share_skew") {
    return ScorerKind::FairnessShareSkew;
  }
  return std::nullopt;
}

std::optional<ScenarioRole> parse_scenario_role(std::string_view token) {
  if (token == "leaderboard") {
    return ScenarioRole::Leaderboard;
  }
  if (token == "gate") {
    return ScenarioRole::Gate;
  }
  return std::nullopt;
}

std::optional<TemplateArgValue> parse_template_arg_value(std::string_view token) {
  if (const auto integer = parse_u64(token); integer.has_value()) {
    return TemplateArgValue{*integer};
  }
  if (const auto duration = parse_duration(token); duration.has_value()) {
    return TemplateArgValue{*duration};
  }
  if (is_identifier_token(token)) {
    return TemplateArgValue{std::string(token)};
  }
  return std::nullopt;
}

std::optional<TemplateParamRef> parse_template_param_ref(std::string_view token) {
  if (token.size() < 2 || token.front() != '$') {
    return std::nullopt;
  }
  const std::string_view name = token.substr(1);
  if (!is_identifier_token(name)) {
    return std::nullopt;
  }
  return TemplateParamRef{
      .name = std::string(name),
  };
}

class Parser {
public:
  explicit Parser(std::string_view source) : source_(source) {}

  ParseResult parse() {
    const auto tokenized = tokenize_lines();
    if (!tokenized.has_value()) {
      return fail_result();
    }

    lines_ = std::move(*tokenized);
    while (index_ < lines_.size()) {
      const ParsedLine& line = lines_[index_];
      if (line.indent != 0) {
        fail(line.line, "malformed indentation at top level");
        return fail_result();
      }
      if (line.tokens.empty()) {
        ++index_;
        continue;
      }

      const std::string& keyword = line.tokens[0];
      if (keyword == "track") {
        if (!parse_track_decl(line)) {
          return fail_result();
        }
        ++index_;
        continue;
      }
      if (keyword == "workers") {
        if (!parse_workers_decl(line)) {
          return fail_result();
        }
        ++index_;
        continue;
      }
      if (keyword == "score") {
        if (!parse_score_decl(line)) {
          return fail_result();
        }
        ++index_;
        continue;
      }
      if (keyword == "role") {
        if (!parse_role_decl(line)) {
          return fail_result();
        }
        ++index_;
        continue;
      }
      if (keyword == "score_groups") {
        if (!parse_score_groups_decl(line)) {
          return fail_result();
        }
        ++index_;
        continue;
      }
      if (keyword == "scenario_weight") {
        if (!parse_scenario_weight_decl(line)) {
          return fail_result();
        }
        ++index_;
        continue;
      }
      if (keyword == "cpu_rate") {
        if (!parse_cpu_rate_decl(line)) {
          return fail_result();
        }
        ++index_;
        continue;
      }
      if (keyword == "switch_cost") {
        if (!parse_switch_cost_decl(line)) {
          return fail_result();
        }
        ++index_;
        continue;
      }
      if (keyword == "node") {
        if (!parse_node_decl(line)) {
          return fail_result();
        }
        ++index_;
        continue;
      }
      if (keyword == "migration_cost") {
        if (!parse_migration_cost_decl(line)) {
          return fail_result();
        }
        ++index_;
        continue;
      }
      if (keyword == "param") {
        if (!parse_param_decl(line)) {
          return fail_result();
        }
        ++index_;
        continue;
      }
      if (keyword == "variant") {
        if (!parse_variant_decl(line)) {
          return fail_result();
        }
        continue;
      }
      if (keyword == "device") {
        if (!parse_device_decl(line)) {
          return fail_result();
        }
        ++index_;
        continue;
      }
      if (keyword == "event") {
        if (!parse_event_decl(line)) {
          return fail_result();
        }
        ++index_;
        continue;
      }
      if (keyword == "latch") {
        if (!parse_latch_decl(line)) {
          return fail_result();
        }
        ++index_;
        continue;
      }
      if (keyword == "template") {
        if (!parse_template_decl(line)) {
          return fail_result();
        }
        continue;
      }
      if (keyword == "group") {
        if (!parse_group_decl(line, &spec_.groups)) {
          return fail_result();
        }
        continue;
      }
      if (keyword == "phase") {
        if (!parse_phase_decl(line)) {
          return fail_result();
        }
        continue;
      }

      fail(line.line, "unknown keyword '" + keyword + "'");
      return fail_result();
    }

    if (!validate_required_declarations()) {
      return fail_result();
    }
    if (!validate_semantics()) {
      return fail_result();
    }

    return ParseResult{
        .spec = spec_,
        .error = std::nullopt,
    };
  }

private:
  std::optional<std::vector<ParsedLine>> tokenize_lines() {
    std::vector<ParsedLine> lines;
    std::size_t current_line = 1;
    std::string current;
    current.reserve(128);

    auto flush_line = [&](std::string raw) -> bool {
      if (!raw.empty() && raw.back() == '\r') {
        raw.pop_back();
      }
      raw = trim_right(std::move(raw));
      if (raw.empty()) {
        return true;
      }

      std::size_t indent = 0;
      while (indent < raw.size() && raw[indent] == ' ') {
        ++indent;
      }
      if (indent < raw.size() && raw[indent] == '\t') {
        fail(current_line, "tabs are not allowed in indentation");
        return false;
      }
      for (char ch : raw) {
        if (ch == '\t') {
          fail(current_line, "tabs are not supported");
          return false;
        }
      }

      std::string content = raw.substr(indent);
      if (content.empty() || content[0] == '#') {
        return true;
      }

      bool opens_block = false;
      if (!content.empty() && content.back() == ':') {
        opens_block = true;
        content.pop_back();
        content = trim_right(std::move(content));
      }
      if (content.empty()) {
        fail(current_line, "missing statement before ':'");
        return false;
      }

      std::vector<std::string> tokens = split_tokens(content);
      if (tokens.empty()) {
        return true;
      }
      lines.push_back(ParsedLine{
          .line = current_line,
          .indent = indent,
          .opens_block = opens_block,
          .tokens = std::move(tokens),
      });
      return true;
    };

    for (char ch : source_) {
      if (ch == '\n') {
        if (!flush_line(current)) {
          return std::nullopt;
        }
        current.clear();
        ++current_line;
        continue;
      }
      current.push_back(ch);
    }
    if (!flush_line(current)) {
      return std::nullopt;
    }

    return lines;
  }

  bool parse_track_decl(const ParsedLine& line) {
    if (line.opens_block || line.tokens.size() != 2) {
      fail(line.line, "track declaration must be `track <value>`");
      return false;
    }
    if (spec_.track.has_value()) {
      fail(line.line, "duplicate track declaration");
      return false;
    }
    const auto track = parse_track(line.tokens[1]);
    if (!track.has_value()) {
      fail(line.line, "unknown track '" + line.tokens[1] + "'");
      return false;
    }
    spec_.track = *track;
    return true;
  }

  bool parse_workers_decl(const ParsedLine& line) {
    if (line.opens_block || line.tokens.size() != 2) {
      fail(line.line, "workers declaration must be `workers <count>`");
      return false;
    }
    if (spec_.workers.has_value()) {
      fail(line.line, "duplicate workers declaration");
      return false;
    }
    const auto workers = parse_u64(line.tokens[1]);
    if (!workers.has_value()) {
      fail(line.line, "workers must be an integer");
      return false;
    }
    spec_.workers = *workers;
    return true;
  }

  bool parse_score_decl(const ParsedLine& line) {
    if (line.opens_block || line.tokens.size() != 2) {
      fail(line.line, "score declaration must be `score <kind>`");
      return false;
    }
    if (spec_.scorer.has_value()) {
      fail(line.line, "duplicate score declaration");
      return false;
    }
    const auto scorer = parse_scorer_kind(line.tokens[1]);
    if (!scorer.has_value()) {
      fail(line.line, "unknown scorer '" + line.tokens[1] + "'");
      return false;
    }
    spec_.scorer = *scorer;
    return true;
  }

  bool parse_role_decl(const ParsedLine& line) {
    if (line.opens_block || line.tokens.size() != 2) {
      fail(line.line, "role declaration must be `role <gate|leaderboard>`");
      return false;
    }
    if (spec_.role.has_value()) {
      fail(line.line, "duplicate role declaration");
      return false;
    }
    const auto role = parse_scenario_role(line.tokens[1]);
    if (!role.has_value()) {
      fail(line.line, "unknown role '" + line.tokens[1] + "'");
      return false;
    }
    spec_.role = *role;
    return true;
  }

  bool parse_score_groups_decl(const ParsedLine& line) {
    if (line.opens_block || line.tokens.size() < 2) {
      fail(line.line, "score_groups declaration must be `score_groups <group> [group ...]`");
      return false;
    }
    if (!spec_.score_groups.empty()) {
      fail(line.line, "duplicate score_groups declaration");
      return false;
    }
    std::unordered_set<std::string> seen;
    for (std::size_t i = 1; i < line.tokens.size(); ++i) {
      const std::string& name = line.tokens[i];
      if (!is_identifier_token(name)) {
        fail(line.line, "score_groups entries must be identifiers");
        return false;
      }
      if (!seen.insert(name).second) {
        fail(line.line, "duplicate score_groups entry '" + name + "'");
        return false;
      }
      spec_.score_groups.push_back(name);
    }
    return true;
  }

  bool parse_scenario_weight_decl(const ParsedLine& line) {
    if (line.opens_block || line.tokens.size() != 3 || line.tokens[1] != "=") {
      fail(line.line, "scenario_weight declaration must be `scenario_weight = <value>`");
      return false;
    }
    if (spec_.scenario_weight.has_value()) {
      fail(line.line, "duplicate scenario_weight declaration");
      return false;
    }
    const auto weight = parse_f64(line.tokens[2]);
    if (!weight.has_value() || *weight <= 0.0) {
      fail(line.line, "scenario_weight must be a positive number");
      return false;
    }
    spec_.scenario_weight = *weight;
    return true;
  }

  bool parse_cpu_rate_decl(const ParsedLine& line) {
    if (line.opens_block || line.tokens.size() != 2) {
      fail(line.line, "cpu_rate declaration must be `cpu_rate <value>`");
      return false;
    }
    if (spec_.cpu_rate.has_value()) {
      fail(line.line, "duplicate cpu_rate declaration");
      return false;
    }
    const auto rate = parse_u64(line.tokens[1]);
    if (!rate.has_value()) {
      fail(line.line, "cpu_rate must be an integer");
      return false;
    }
    spec_.cpu_rate = *rate;
    return true;
  }

  bool parse_switch_cost_decl(const ParsedLine& line) {
    if (line.opens_block || line.tokens.size() != 2) {
      fail(line.line, "switch_cost declaration must be `switch_cost <value>`");
      return false;
    }
    if (spec_.switch_cost_us.has_value()) {
      fail(line.line, "duplicate switch_cost declaration");
      return false;
    }
    const auto cost = parse_u64(line.tokens[1]);
    if (!cost.has_value() || *cost == 0) {
      fail(line.line, "switch_cost must be a positive integer");
      return false;
    }
    spec_.switch_cost_us = *cost;
    return true;
  }

  bool parse_node_decl(const ParsedLine& line) {
    if (line.opens_block || line.tokens.size() != 3) {
      fail(line.line, "node declaration must be `node <name> <begin-end>`");
      return false;
    }
    if (!is_identifier_token(line.tokens[1])) {
      fail(line.line, "node name must be an identifier");
      return false;
    }
    const auto range = parse_worker_range(line.tokens[2]);
    if (!range.has_value()) {
      fail(line.line, "node worker range must be `<begin-end>`");
      return false;
    }
    spec_.nodes.push_back(NodeSpec{
        .name = line.tokens[1],
        .worker_begin = range->first,
        .worker_end = range->second,
    });
    node_declarations_.push_back(NodeDeclaration{
        .line = line.line,
        .name = line.tokens[1],
        .worker_begin = range->first,
        .worker_end = range->second,
    });
    return true;
  }

  bool parse_migration_cost_decl(const ParsedLine& line) {
    migration_cost_line_ = line.line;
    if (line.opens_block || (line.tokens.size() != 2 && line.tokens.size() != 3)) {
      fail(line.line, "migration_cost declaration must be `migration_cost <value>` or "
                      "`migration_cost local=<x> remote=<y>`");
      return false;
    }
    if (spec_.migration_cost_us.has_value() || spec_.migration_cost_local_us.has_value() ||
        spec_.migration_cost_remote_us.has_value()) {
      fail(line.line, "duplicate migration_cost declaration");
      return false;
    }
    if (line.tokens.size() == 2) {
      const auto cost = parse_u64(line.tokens[1]);
      if (!cost.has_value() || *cost == 0) {
        fail(line.line, "migration_cost must be a positive integer");
        return false;
      }
      spec_.migration_cost_us = *cost;
      return true;
    }

    auto parse_named_cost = [](std::string_view token,
                               std::string_view name) -> std::optional<uint64_t> {
      const std::string prefix = std::string(name) + "=";
      if (!token.starts_with(prefix)) {
        return std::nullopt;
      }
      return parse_u64(token.substr(prefix.size()));
    };

    const auto local = parse_named_cost(line.tokens[1], "local");
    const auto remote = parse_named_cost(line.tokens[2], "remote");
    if (!local.has_value() || !remote.has_value()) {
      fail(line.line, "migration_cost with topology must be `migration_cost local=<x> remote=<y>`");
      return false;
    }
    if (*local == 0 || *remote == 0) {
      fail(line.line, "migration_cost local/remote values must be positive integers");
      return false;
    }
    spec_.migration_cost_local_us = *local;
    spec_.migration_cost_remote_us = *remote;
    return true;
  }

  bool parse_param_decl(const ParsedLine& line) {
    if (line.opens_block || line.tokens.size() != 4 || line.tokens[2] != "=") {
      fail(line.line, "param declaration must be `param <name> = <value>`");
      return false;
    }
    const std::string& name = line.tokens[1];
    if (!is_identifier_token(name)) {
      fail(line.line, "param name must be an identifier");
      return false;
    }
    if (known_param_names_.find(name) != known_param_names_.end()) {
      fail(line.line, "duplicate param name '" + name + "'");
      return false;
    }
    const auto value = parse_template_arg_value(line.tokens[3]);
    if (!value.has_value()) {
      fail(line.line, "param value must be an integer, duration, or identifier");
      return false;
    }
    known_param_names_.insert(name);
    spec_.params.push_back(ParamSpec{
        .name = name,
        .default_value = *value,
    });
    param_declarations_.push_back(NamedDeclaration{
        .line = line.line,
        .name = name,
    });
    return true;
  }

  bool parse_variant_decl(const ParsedLine& line) {
    if (!line.opens_block || line.tokens.size() != 2) {
      fail(line.line, "variant declaration must be `variant <name>:`");
      return false;
    }
    const std::string& name = line.tokens[1];
    if (!is_identifier_token(name)) {
      fail(line.line, "variant name must be an identifier");
      return false;
    }
    if (known_variant_names_.find(name) != known_variant_names_.end()) {
      fail(line.line, "duplicate variant name '" + name + "'");
      return false;
    }

    VariantSpec variant{
        .name = name,
    };
    ++index_;
    if (index_ >= lines_.size() || lines_[index_].indent <= line.indent) {
      fail(line.line, "expected an indented block");
      return false;
    }
    const std::size_t child_indent = lines_[index_].indent;
    std::unordered_set<std::string> seen_overrides;
    while (index_ < lines_.size()) {
      const ParsedLine& nested = lines_[index_];
      if (nested.indent <= line.indent) {
        break;
      }
      if (nested.indent != child_indent) {
        fail(nested.line, "malformed indentation");
        return false;
      }
      if (nested.tokens.empty()) {
        ++index_;
        continue;
      }
      if (nested.opens_block || nested.tokens.size() != 3 || nested.tokens[1] != "=") {
        fail(nested.line, "variant override must be `<param> = <value>`");
        return false;
      }
      const std::string& override_name = nested.tokens[0];
      if (!is_identifier_token(override_name)) {
        fail(nested.line, "variant override name must be an identifier");
        return false;
      }
      if (!seen_overrides.insert(override_name).second) {
        fail(nested.line, "duplicate variant override '" + override_name + "'");
        return false;
      }
      const auto value = parse_template_arg_value(nested.tokens[2]);
      if (!value.has_value()) {
        fail(nested.line, "variant override value must be an integer, duration, or identifier");
        return false;
      }
      variant.overrides.push_back(VariantOverride{
          .name = override_name,
          .value = *value,
      });
      variant_override_references_.push_back(VariantOverrideReference{
          .line = nested.line,
          .variant_name = name,
          .param_name = override_name,
      });
      ++index_;
    }

    known_variant_names_.insert(name);
    spec_.variants.push_back(std::move(variant));
    variant_declarations_.push_back(NamedDeclaration{
        .line = line.line,
        .name = name,
    });
    return true;
  }

  bool parse_params(const ParsedLine& line, std::size_t start, std::vector<Param>* params) {
    for (std::size_t i = start; i < line.tokens.size(); ++i) {
      const std::string& token = line.tokens[i];
      const std::size_t eq = token.find('=');
      if (eq == std::string::npos || eq == 0 || eq + 1 >= token.size()) {
        fail(line.line, "invalid parameter '" + token + "', expected key=value");
        return false;
      }
      params->push_back(Param{
          .key = token.substr(0, eq),
          .value = token.substr(eq + 1),
      });
    }
    return true;
  }

  bool parse_device_decl(const ParsedLine& line) {
    if (line.opens_block || line.tokens.size() < 3) {
      fail(line.line, "device declaration must be `device <name> <model> [key=value ...]`");
      return false;
    }

    DeviceSpec device{
        .name = line.tokens[1],
        .model = line.tokens[2],
    };
    if (!parse_params(line, 3, &device.params)) {
      return false;
    }

    for (const Param& param : device.params) {
      if (param.key == "rate") {
        if (device.rate.has_value()) {
          fail(line.line, "duplicate device rate parameter");
          return false;
        }
        const auto rate = parse_u64(param.value);
        if (!rate.has_value() || *rate == 0) {
          fail(line.line, "device rate must be a positive integer");
          return false;
        }
        device.rate = *rate;
        continue;
      }
      if (param.key != "parallel") {
        continue;
      }
      const auto parallel = parse_u64(param.value);
      if (!parallel.has_value() || *parallel == 0) {
        fail(line.line, "device parallel must be a positive integer");
        return false;
      }
      device.parallelism = *parallel;
    }

    spec_.devices.push_back(std::move(device));
    device_declarations_.push_back(NamedDeclaration{
        .line = line.line,
        .name = spec_.devices.back().name,
    });
    return true;
  }

  bool parse_event_decl(const ParsedLine& line) {
    if (line.opens_block || line.tokens.size() != 2) {
      fail(line.line, "event declaration must be `event <name>`");
      return false;
    }
    const std::string& name = line.tokens[1];
    if (!is_identifier_token(name)) {
      fail(line.line, "event name must be an identifier");
      return false;
    }
    for (const NamedDeclaration& decl : event_declarations_) {
      if (decl.name == name) {
        fail(line.line, "duplicate event name '" + name + "'");
        return false;
      }
    }
    spec_.events.push_back(SyncEvent{.name = name});
    event_declarations_.push_back(NamedDeclaration{.line = line.line, .name = name});
    return true;
  }

  bool parse_latch_decl(const ParsedLine& line) {
    if (line.opens_block || line.tokens.size() != 3) {
      fail(line.line, "latch declaration must be `latch <name> count=<value>`");
      return false;
    }
    const std::string& name = line.tokens[1];
    if (!is_identifier_token(name)) {
      fail(line.line, "latch name must be an identifier");
      return false;
    }
    const std::string& count_token = line.tokens[2];
    constexpr char prefix[] = "count=";
    if (count_token.size() <= sizeof(prefix) - 1 ||
        count_token.substr(0, sizeof(prefix) - 1) != prefix) {
      fail(line.line, "latch declaration must include 'count='");
      return false;
    }
    const auto count = parse_u64(count_token.substr(sizeof(prefix) - 1));
    if (!count.has_value() || *count == 0) {
      fail(line.line, "latch count must be a positive integer");
      return false;
    }
    for (const NamedDeclaration& decl : latch_declarations_) {
      if (decl.name == name) {
        fail(line.line, "duplicate latch name '" + name + "'");
        return false;
      }
    }
    spec_.latches.push_back(SyncLatch{.name = name, .count = *count});
    latch_declarations_.push_back(NamedDeclaration{.line = line.line, .name = name});
    return true;
  }

  bool parse_group_arrival(const ParsedLine& line, std::size_t token_index,
                           std::optional<ArrivalPolicy>* arrival) {
    if (token_index >= line.tokens.size() || line.tokens[token_index] != "arrival") {
      fail(line.line, "group declaration must be `group <name> * <count> [arrival ...]:`");
      return false;
    }
    if (token_index + 1 >= line.tokens.size()) {
      fail(line.line, "malformed arrival clause");
      return false;
    }

    const std::string& arrival_kind = line.tokens[token_index + 1];
    if (arrival_kind == "delay" || arrival_kind == "stagger" || arrival_kind == "interval") {
      if (token_index + 3 != line.tokens.size()) {
        fail(line.line, "malformed arrival clause");
        return false;
      }
      const auto duration = parse_duration(line.tokens[token_index + 2]);
      if (!duration.has_value()) {
        fail(line.line, "malformed arrival duration '" + line.tokens[token_index + 2] + "'");
        return false;
      }
      ArrivalPolicy policy{};
      if (arrival_kind == "delay") {
        policy.policy = ArrivalPolicyKind::Delay;
      } else if (arrival_kind == "stagger") {
        policy.policy = ArrivalPolicyKind::Stagger;
      } else {
        policy.policy = ArrivalPolicyKind::Interval;
      }
      policy.duration = *duration;
      *arrival = std::move(policy);
      return true;
    }

    if (arrival_kind == "burst") {
      if (token_index + 5 != line.tokens.size() || line.tokens[token_index + 3] != "every") {
        fail(line.line, "malformed arrival clause");
        return false;
      }
      const auto burst_size = parse_u64(line.tokens[token_index + 2]);
      if (!burst_size.has_value() || *burst_size == 0) {
        fail(line.line, "arrival burst size must be a positive integer");
        return false;
      }
      const auto duration = parse_duration(line.tokens[token_index + 4]);
      if (!duration.has_value()) {
        fail(line.line, "malformed arrival duration '" + line.tokens[token_index + 4] + "'");
        return false;
      }
      ArrivalPolicy policy{
          .policy = ArrivalPolicyKind::Burst,
          .duration = *duration,
          .burst_size = *burst_size,
      };
      *arrival = std::move(policy);
      return true;
    }

    fail(line.line, "unknown arrival policy '" + arrival_kind + "'");
    return false;
  }

  bool parse_dependency_expression(const ParsedLine& line, std::size_t token_index,
                                   DependencyExpr* expr, std::size_t* next_index) {
    if (token_index >= line.tokens.size()) {
      fail(line.line, "missing dependency expression after 'after'");
      return false;
    }
    const std::string& token = line.tokens[token_index];
    std::string name;
    std::vector<std::string> args;
    std::string merged_token = token;
    std::size_t consumed_index = token_index + 1;
    if (const auto collected = collect_balanced_token_sequence(line.tokens, token_index);
        collected.has_value()) {
      merged_token = collected->first;
      consumed_index = collected->second;
    }
    if (parse_call_like_token(merged_token, &name, &args)) {
      if (name == "all") {
        if (args.empty()) {
          fail(line.line, "`all` must list at least one dependency");
          return false;
        }
        expr->kind = DependencyKind::All;
        expr->targets = std::move(args);
      } else if (name == "each") {
        if (args.size() != 1) {
          fail(line.line, "`each` requires exactly one dependency");
          return false;
        }
        expr->kind = DependencyKind::Each;
        expr->targets = {std::move(args[0])};
      } else if (name == "fanout") {
        if (args.size() != 1) {
          fail(line.line, "`fanout` requires exactly one group");
          return false;
        }
        expr->kind = DependencyKind::Fanout;
        expr->targets = {std::move(args[0])};
      } else if (name == "join") {
        if (args.size() != 1) {
          fail(line.line, "`join` requires exactly one group");
          return false;
        }
        expr->kind = DependencyKind::Join;
        expr->targets = {std::move(args[0])};
      } else {
        fail(line.line, "unknown dependency function '" + name + "'");
        return false;
      }
      *next_index = consumed_index;
      return true;
    }
    if (!is_identifier_token(token)) {
      fail(line.line, "dependency target must be an identifier or a call");
      return false;
    }
    expr->kind = DependencyKind::Name;
    expr->targets = {token};
    *next_index = token_index + 1;
    return true;
  }

  bool parse_group_decl(const ParsedLine& line, std::vector<GroupSpec>* out_groups) {
    if (!line.opens_block || line.tokens.size() < 4 || line.tokens[2] != "*") {
      fail(line.line, "group declaration must be `group <name> * <count> [arrival ...]:`");
      return false;
    }
    const auto count = parse_u64(line.tokens[3]);
    if (!count.has_value()) {
      fail(line.line, "group count must be an integer");
      return false;
    }

    GroupSpec group{
        .name = line.tokens[1],
        .count = *count,
    };
    std::size_t token_index = 4;
    while (token_index < line.tokens.size()) {
      const std::string& token = line.tokens[token_index];
      if (token == "after") {
        if (group.dependency.has_value()) {
          fail(line.line, "duplicate group dependency clause");
          return false;
        }
        DependencyExpr dependency{};
        if (!parse_dependency_expression(line, token_index + 1, &dependency, &token_index)) {
          return false;
        }
        group.dependency = std::move(dependency);
        continue;
      }
      if (token.rfind("weight=", 0) == 0) {
        if (group.weight != 1) {
          fail(line.line, "duplicate group weight");
          return false;
        }
        const auto weight = parse_u64(std::string_view(token).substr(sizeof("weight=") - 1));
        if (!weight.has_value() || *weight == 0) {
          fail(line.line, "group weight must be a positive integer");
          return false;
        }
        group.weight = *weight;
        ++token_index;
        continue;
      }
      if (!parse_group_arrival(line, /*token_index=*/token_index, &group.arrival)) {
        return false;
      }
      token_index = line.tokens.size();
    }

    ++index_;
    if (!parse_block(line.indent, line.line, &group.body)) {
      return false;
    }
    out_groups->push_back(std::move(group));
    group_declarations_.push_back(NamedDeclaration{
        .line = line.line,
        .name = out_groups->back().name,
    });
    return true;
  }

  bool parse_template_decl(const ParsedLine& line) {
    if (!line.opens_block || line.tokens.size() < 2) {
      fail(line.line, "template declaration must be `template <name>(<param,...>):`");
      return false;
    }

    const std::string signature = concat_tokens_without_spaces(line.tokens, 1);
    std::string template_name;
    std::vector<std::string> params;
    if (!parse_call_like_token(signature, &template_name, &params)) {
      fail(line.line, "template declaration must be `template <name>(<param,...>):`");
      return false;
    }
    for (const std::string& param : params) {
      if (!is_identifier_token(param)) {
        fail(line.line, "template parameter must be an identifier");
        return false;
      }
    }

    TemplateSpec templ{
        .name = std::move(template_name),
        .params = std::move(params),
    };
    std::unordered_set<std::string> seen_params;
    for (const std::string& param_name : templ.params) {
      if (!seen_params.insert(param_name).second) {
        fail(line.line, "duplicate template parameter '" + param_name + "'");
        return false;
      }
    }
    ++index_;
    template_param_stack_.push_back(templ.params);
    const bool block_ok = parse_block(line.indent, line.line, &templ.body);
    template_param_stack_.pop_back();
    if (!block_ok) {
      return false;
    }

    spec_.templates.push_back(std::move(templ));
    template_declarations_.push_back(TemplateDeclaration{
        .line = line.line,
        .name = spec_.templates.back().name,
        .param_count = spec_.templates.back().params.size(),
    });
    return true;
  }

  bool parse_phase_decl(const ParsedLine& line) {
    if (!line.opens_block || line.tokens.size() != 4 || line.tokens[2] != "at") {
      fail(line.line, "phase declaration must be `phase <name> at <duration>:`");
      return false;
    }
    const auto at = parse_duration(line.tokens[3]);
    if (!at.has_value()) {
      fail(line.line, "malformed phase start duration '" + line.tokens[3] + "'");
      return false;
    }

    PhaseSpec phase{
        .name = line.tokens[1],
        .at = *at,
    };

    ++index_;
    if (index_ >= lines_.size() || lines_[index_].indent <= line.indent) {
      fail(line.line, "expected an indented block");
      return false;
    }
    const std::size_t child_indent = lines_[index_].indent;

    while (index_ < lines_.size()) {
      const ParsedLine& nested = lines_[index_];
      if (nested.indent <= line.indent) {
        break;
      }
      if (nested.indent != child_indent) {
        fail(nested.line, "malformed indentation");
        return false;
      }
      if (nested.tokens.empty()) {
        ++index_;
        continue;
      }
      if (nested.tokens[0] != "group") {
        fail(nested.line, "phase blocks only support group declarations");
        return false;
      }
      if (!parse_group_decl(nested, &phase.groups)) {
        return false;
      }
    }

    spec_.phases.push_back(std::move(phase));
    phase_declarations_.push_back(NamedDeclaration{
        .line = line.line,
        .name = spec_.phases.back().name,
    });
    return true;
  }

  bool parse_block(std::size_t parent_indent, std::size_t owner_line, std::vector<Operation>* out) {
    if (index_ >= lines_.size() || lines_[index_].indent <= parent_indent) {
      fail(owner_line, "expected an indented block");
      return false;
    }
    const std::size_t child_indent = lines_[index_].indent;

    while (index_ < lines_.size()) {
      const ParsedLine& line = lines_[index_];
      if (line.indent <= parent_indent) {
        break;
      }
      if (line.indent != child_indent) {
        fail(line.line, "malformed indentation");
        return false;
      }
      if (!parse_statement(line, child_indent, out)) {
        return false;
      }
    }

    return true;
  }

  bool parse_statement(const ParsedLine& line, std::size_t current_indent,
                       std::vector<Operation>* out) {
    if (line.tokens.empty()) {
      ++index_;
      return true;
    }

    const std::string& keyword = line.tokens[0];
    if (keyword == "repeat") {
      return parse_repeat_statement(line, current_indent, out);
    }
    if (keyword == "compute") {
      return parse_compute_statement(line, out);
    }
    if (keyword == "call") {
      return parse_call_statement(line, out);
    }
    if (keyword == "sleep") {
      return parse_sleep_statement(line, out);
    }
    if (keyword == "choice") {
      return parse_choice_statement(line, out);
    }
    if (keyword == "use") {
      return parse_use_statement(line, out);
    }
    if (keyword == "wait") {
      return parse_wait_statement(line, out);
    }
    if (keyword == "signal") {
      return parse_signal_statement(line, out);
    }
    if (keyword == "arrive") {
      return parse_arrive_statement(line, out);
    }

    fail(line.line, "unknown keyword '" + keyword + "'");
    return false;
  }

  bool parse_repeat_statement(const ParsedLine& line, std::size_t current_indent,
                              std::vector<Operation>* out) {
    if (!line.opens_block || line.tokens.size() != 2) {
      fail(line.line, "repeat statement must be `repeat <count>:`");
      return false;
    }
    const auto count = parse_u64(line.tokens[1]);
    if (!count.has_value()) {
      fail(line.line, "repeat count must be an integer");
      return false;
    }
    auto repeat = std::make_shared<RepeatOp>();
    repeat->count = *count;

    ++index_;
    if (!parse_block(current_indent, line.line, &repeat->body)) {
      return false;
    }

    out->push_back(repeat);
    return true;
  }

  bool parse_choice_statement(const ParsedLine& line, std::vector<Operation>* out) {
    if (!line.opens_block || line.tokens.size() != 1) {
      fail(line.line, "choice must be declared as `choice:`");
      return false;
    }
    if (choice_depth_ > 0) {
      fail(line.line, "nested choice blocks are not supported");
      return false;
    }

    ++choice_depth_;
    ++index_;
    std::vector<WeightedBranch> branches;
    const bool block_ok = parse_choice_block(line.indent, line.line, &branches);
    --choice_depth_;
    if (!block_ok) {
      return false;
    }
    if (branches.empty()) {
      fail(line.line, "choice must declare at least one branch");
      return false;
    }

    ChoiceOp choice;
    choice.branches = std::move(branches);
    out->push_back(std::move(choice));
    return true;
  }

  bool parse_choice_block(std::size_t parent_indent, std::size_t owner_line,
                          std::vector<WeightedBranch>* branches) {
    if (index_ >= lines_.size() || lines_[index_].indent <= parent_indent) {
      fail(owner_line, "choice must include an indented branch block");
      return false;
    }
    const std::size_t child_indent = lines_[index_].indent;

    while (index_ < lines_.size()) {
      const ParsedLine& line = lines_[index_];
      if (line.indent <= parent_indent) {
        break;
      }
      if (line.indent != child_indent) {
        fail(line.line, "malformed indentation in choice");
        return false;
      }
      if (line.tokens.empty()) {
        ++index_;
        continue;
      }
      if (line.tokens[0] != "weight") {
        fail(line.line, "choice branch must start with 'weight'");
        return false;
      }
      if (!line.opens_block || line.tokens.size() != 2) {
        fail(line.line, "choice branch must be `weight <value>:`");
        return false;
      }
      const auto weight = parse_u64(line.tokens[1]);
      if (!weight.has_value() || *weight == 0) {
        fail(line.line, "choice weight must be a positive integer");
        return false;
      }
      ++index_;
      std::vector<Operation> branch_body;
      if (!parse_block(line.indent, line.line, &branch_body)) {
        return false;
      }
      branches->push_back(WeightedBranch{
          .weight = *weight,
          .body = std::move(branch_body),
      });
    }

    return true;
  }

  bool parse_compute_statement(const ParsedLine& line, std::vector<Operation>* out) {
    if (line.opens_block || line.tokens.size() < 2) {
      fail(line.line, "compute statement must be `compute <units> [key=value ...]`");
      return false;
    }
    ComputeOp compute{};
    const auto units = parse_u64(line.tokens[1]);
    if (units.has_value()) {
      compute.units = *units;
    } else {
      const auto units_param_ref = parse_template_param_ref(line.tokens[1]);
      if (!units_param_ref.has_value()) {
        fail(line.line, "compute units must be an integer or a template parameter reference");
        return false;
      }
      if (!validate_template_param_ref(*units_param_ref, line, "compute units")) {
        return false;
      }
      compute.units_param_ref = *units_param_ref;
    }
    if (!parse_params(line, 2, &compute.params)) {
      return false;
    }

    out->push_back(std::move(compute));
    ++index_;
    return true;
  }

  bool parse_call_statement(const ParsedLine& line, std::vector<Operation>* out) {
    if (line.opens_block || line.tokens.size() < 3) {
      fail(line.line, "call statement must be `call <device> <service> [key=value ...]`");
      return false;
    }
    CallOp call{};
    if (const auto device_param_ref = parse_template_param_ref(line.tokens[1]);
        device_param_ref.has_value()) {
      if (!validate_template_param_ref(*device_param_ref, line, "call device")) {
        return false;
      }
      call.device_param_ref = *device_param_ref;
    } else {
      call.device = line.tokens[1];
    }
    const auto service_units = parse_u64(line.tokens[2]);
    if (service_units.has_value()) {
      call.service_units = *service_units;
    } else {
      const auto service_param_ref = parse_template_param_ref(line.tokens[2]);
      if (!service_param_ref.has_value()) {
        fail(line.line, "call service units must be an integer or a template parameter reference");
        return false;
      }
      if (!validate_template_param_ref(*service_param_ref, line, "call service units")) {
        return false;
      }
      call.service_units_param_ref = *service_param_ref;
    }
    if (!parse_params(line, 3, &call.params)) {
      return false;
    }
    if (!call.device_param_ref.has_value()) {
      call_references_.push_back(CallReference{
          .line = line.line,
          .device = call.device,
      });
    }

    out->push_back(std::move(call));
    ++index_;
    return true;
  }

  bool parse_sleep_statement(const ParsedLine& line, std::vector<Operation>* out) {
    if (line.opens_block || line.tokens.size() < 2) {
      fail(line.line, "sleep statement must be `sleep <duration> [key=value ...]`");
      return false;
    }
    SleepOp sleep{};
    const auto duration = parse_duration(line.tokens[1]);
    if (duration.has_value()) {
      sleep.duration = *duration;
    } else {
      const auto duration_param_ref = parse_template_param_ref(line.tokens[1]);
      if (!duration_param_ref.has_value()) {
        fail(line.line, "malformed duration '" + line.tokens[1] + "'");
        return false;
      }
      if (!validate_template_param_ref(*duration_param_ref, line, "sleep duration")) {
        return false;
      }
      sleep.duration_param_ref = *duration_param_ref;
    }
    if (!parse_params(line, 2, &sleep.params)) {
      return false;
    }

    out->push_back(std::move(sleep));
    ++index_;
    return true;
  }

  bool parse_use_statement(const ParsedLine& line, std::vector<Operation>* out) {
    if (line.opens_block || line.tokens.size() < 2) {
      fail(line.line, "use statement must be `use <template>(<arg,...>)`");
      return false;
    }

    const std::string invocation = concat_tokens_without_spaces(line.tokens, 1);
    std::string template_name;
    std::vector<std::string> raw_args;
    if (!parse_call_like_token(invocation, &template_name, &raw_args)) {
      fail(line.line, "use statement must be `use <template>(<arg,...>)`");
      return false;
    }

    UseOp use{
        .template_name = std::move(template_name),
    };
    use.args.reserve(raw_args.size());
    for (const std::string& arg : raw_args) {
      const auto parsed_arg = parse_template_arg_value(arg);
      if (!parsed_arg.has_value()) {
        fail(line.line, "unsupported use argument '" + arg + "'");
        return false;
      }
      use.args.push_back(*parsed_arg);
    }

    use_references_.push_back(UseReference{
        .line = line.line,
        .template_name = use.template_name,
        .arg_count = use.args.size(),
    });
    out->push_back(std::move(use));
    ++index_;
    return true;
  }

  bool parse_wait_statement(const ParsedLine& line, std::vector<Operation>* out) {
    if (line.opens_block || line.tokens.size() != 2) {
      fail(line.line, "wait statement must be `wait <target>`");
      return false;
    }
    if (!is_identifier_token(line.tokens[1])) {
      fail(line.line, "wait target must be an identifier");
      return false;
    }
    out->push_back(WaitOp{.target = line.tokens[1]});
    ++index_;
    return true;
  }

  bool parse_signal_statement(const ParsedLine& line, std::vector<Operation>* out) {
    if (line.opens_block || line.tokens.size() != 2) {
      fail(line.line, "signal statement must be `signal <target>`");
      return false;
    }
    if (!is_identifier_token(line.tokens[1])) {
      fail(line.line, "signal target must be an identifier");
      return false;
    }
    out->push_back(SignalOp{.target = line.tokens[1]});
    ++index_;
    return true;
  }

  bool parse_arrive_statement(const ParsedLine& line, std::vector<Operation>* out) {
    if (line.opens_block || line.tokens.size() != 2) {
      fail(line.line, "arrive statement must be `arrive <target>`");
      return false;
    }
    if (!is_identifier_token(line.tokens[1])) {
      fail(line.line, "arrive target must be an identifier");
      return false;
    }
    out->push_back(ArriveOp{.target = line.tokens[1]});
    ++index_;
    return true;
  }

  bool validate_required_declarations() {
    const std::size_t fallback_line = lines_.empty() ? 0 : lines_.front().line;
    if (!spec_.track.has_value()) {
      fail(fallback_line, "missing required declaration 'track'");
      return false;
    }
    if (!spec_.workers.has_value()) {
      fail(fallback_line, "missing required declaration 'workers'");
      return false;
    }
    if (!spec_.cpu_rate.has_value()) {
      fail(fallback_line, "missing required declaration 'cpu_rate'");
      return false;
    }
    return true;
  }

  bool validate_semantics() {
    if (!spec_.score_groups.empty() && spec_.scorer != ScorerKind::LatencyFlowP99) {
      fail(lines_.empty() ? 0 : lines_.front().line,
           "score_groups requires `score latency_flow_p99`");
      return false;
    }

    std::unordered_set<std::string> known_nodes;
    for (const NodeDeclaration& node_decl : node_declarations_) {
      const auto [_, inserted] = known_nodes.insert(node_decl.name);
      if (!inserted) {
        fail(node_decl.line, "duplicate node name '" + node_decl.name + "'");
        return false;
      }
    }

    if (!spec_.nodes.empty()) {
      if (!spec_.workers.has_value()) {
        fail(node_declarations_.front().line, "node declarations require workers to be declared");
        return false;
      }
      if (!spec_.migration_cost_local_us.has_value() ||
          !spec_.migration_cost_remote_us.has_value() || spec_.migration_cost_us.has_value()) {
        fail(migration_cost_line_.value_or(node_declarations_.front().line),
             "topology requires `migration_cost local=<x> remote=<y>`");
        return false;
      }
      if (*spec_.migration_cost_remote_us < *spec_.migration_cost_local_us) {
        fail(migration_cost_line_.value_or(node_declarations_.front().line),
             "migration_cost remote must be >= local");
        return false;
      }

      std::vector<bool> covered(static_cast<std::size_t>(*spec_.workers), false);
      for (const NodeDeclaration& node_decl : node_declarations_) {
        if (node_decl.worker_end >= *spec_.workers) {
          fail(node_decl.line, "node range exceeds declared worker count");
          return false;
        }
        for (uint64_t worker = node_decl.worker_begin; worker <= node_decl.worker_end; ++worker) {
          const std::size_t index = static_cast<std::size_t>(worker);
          if (covered[index]) {
            fail(node_decl.line, "node ranges must not overlap");
            return false;
          }
          covered[index] = true;
        }
      }
      if (std::find(covered.begin(), covered.end(), false) != covered.end()) {
        fail(node_declarations_.front().line,
             "node declarations must cover every worker exactly once");
        return false;
      }
    } else if (spec_.migration_cost_local_us.has_value() ||
               spec_.migration_cost_remote_us.has_value()) {
      fail(migration_cost_line_.value_or(lines_.empty() ? 0 : lines_.front().line),
           "migration_cost local/remote requires node declarations");
      return false;
    }

    std::unordered_set<std::string> known_params;
    for (const NamedDeclaration& param_decl : param_declarations_) {
      const auto [_, inserted] = known_params.insert(param_decl.name);
      if (!inserted) {
        fail(param_decl.line, "duplicate param name '" + param_decl.name + "'");
        return false;
      }
    }

    std::unordered_set<std::string> known_variants;
    for (const NamedDeclaration& variant_decl : variant_declarations_) {
      if (known_variants.find(variant_decl.name) != known_variants.end()) {
        fail(variant_decl.line, "duplicate variant name '" + variant_decl.name + "'");
        return false;
      }
      known_variants.insert(variant_decl.name);
    }

    for (const VariantOverrideReference& reference : variant_override_references_) {
      if (known_params.find(reference.param_name) != known_params.end()) {
        continue;
      }
      fail(reference.line, "unknown param '" + reference.param_name + "' in variant '" +
                               reference.variant_name + "'");
      return false;
    }

    for (const ParamReference& reference : param_references_) {
      if (known_params.find(reference.name) != known_params.end()) {
        continue;
      }
      if (!reference.template_params.empty() &&
          std::find(reference.template_params.begin(), reference.template_params.end(),
                    reference.name) != reference.template_params.end()) {
        continue;
      }
      if (!reference.template_params.empty()) {
        fail(reference.line, "unknown parameter '" + reference.name + "' in " + reference.context +
                                 "; expected template parameter or top-level param");
        return false;
      }
      fail(reference.line, "unknown param '" + reference.name + "' in " + reference.context);
      return false;
    }

    std::unordered_set<std::string> known_devices;
    for (const NamedDeclaration& device_decl : device_declarations_) {
      const auto [_, inserted] = known_devices.insert(device_decl.name);
      if (!inserted) {
        fail(device_decl.line, "duplicate device name '" + device_decl.name + "'");
        return false;
      }
    }

    std::unordered_set<std::string> known_groups;
    for (const NamedDeclaration& group_decl : group_declarations_) {
      const auto [_, inserted] = known_groups.insert(group_decl.name);
      if (!inserted) {
        fail(group_decl.line, "duplicate group name '" + group_decl.name + "'");
        return false;
      }
    }
    for (const std::string& name : spec_.score_groups) {
      if (known_groups.find(name) == known_groups.end()) {
        fail(lines_.empty() ? 0 : lines_.front().line,
             "score_groups references unknown group '" + name + "'");
        return false;
      }
    }

    std::unordered_set<std::string> known_phases;
    for (const NamedDeclaration& phase_decl : phase_declarations_) {
      const auto [_, inserted] = known_phases.insert(phase_decl.name);
      if (!inserted) {
        fail(phase_decl.line, "duplicate phase name '" + phase_decl.name + "'");
        return false;
      }
    }

    std::unordered_map<std::string, std::size_t> known_templates;
    for (const TemplateDeclaration& template_decl : template_declarations_) {
      const auto [it, inserted] =
          known_templates.emplace(template_decl.name, template_decl.param_count);
      if (!inserted) {
        fail(template_decl.line, "duplicate template name '" + template_decl.name + "'");
        return false;
      }
      (void)it;
    }

    for (const CallReference& call : call_references_) {
      if (known_devices.find(call.device) != known_devices.end()) {
        continue;
      }
      fail(call.line, "unknown device '" + call.device + "'");
      return false;
    }

    for (const UseReference& use : use_references_) {
      const auto it = known_templates.find(use.template_name);
      if (it == known_templates.end()) {
        fail(use.line, "unknown template '" + use.template_name + "'");
        return false;
      }
      if (it->second != use.arg_count) {
        fail(use.line, "template arity mismatch for '" + use.template_name + "': expected " +
                           std::to_string(it->second) + ", got " + std::to_string(use.arg_count));
        return false;
      }
    }

    return true;
  }

  bool validate_template_param_ref(const TemplateParamRef& param, const ParsedLine& line,
                                   std::string_view context) {
    ParamReference reference{
        .line = line.line,
        .name = param.name,
        .context = std::string(context),
    };
    if (!template_param_stack_.empty()) {
      reference.template_params = template_param_stack_.back();
    }
    param_references_.push_back(std::move(reference));
    return true;
  }

  void fail(std::size_t line, std::string message) {
    if (error_.has_value()) {
      return;
    }
    error_ = ParseError{
        .line = line,
        .message = std::move(message),
    };
  }

  ParseResult fail_result() const {
    return ParseResult{
        .spec = std::nullopt,
        .error = error_,
    };
  }

  std::string_view source_;
  std::vector<ParsedLine> lines_;
  std::size_t index_ = 0;
  WorkloadSpec spec_{};
  std::optional<std::size_t> migration_cost_line_;
  std::vector<CallReference> call_references_;
  std::vector<UseReference> use_references_;
  std::vector<NodeDeclaration> node_declarations_;
  std::vector<NamedDeclaration> device_declarations_;
  std::vector<NamedDeclaration> event_declarations_;
  std::vector<NamedDeclaration> latch_declarations_;
  std::vector<NamedDeclaration> param_declarations_;
  std::vector<NamedDeclaration> variant_declarations_;
  std::vector<NamedDeclaration> group_declarations_;
  std::vector<NamedDeclaration> phase_declarations_;
  std::vector<TemplateDeclaration> template_declarations_;
  std::vector<VariantOverrideReference> variant_override_references_;
  std::vector<ParamReference> param_references_;
  std::optional<ParseError> error_;
  std::vector<std::vector<std::string>> template_param_stack_;
  std::unordered_set<std::string> known_param_names_;
  std::unordered_set<std::string> known_variant_names_;
  std::size_t choice_depth_ = 0;
};

} // namespace

ParseResult parse_workload_dsl(std::string_view source) {
  return Parser(source).parse();
}

} // namespace schedlab::benchmark::dsl
