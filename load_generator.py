#!/usr/bin/env python3
import argparse
import json
import os
import random
import string
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import requests


class LoadGenerator:
    def __init__(self, base_url, num_users, duration, min_msg_len, max_msg_len,
                 min_interval, max_interval, feed_ratio, output_dir):
        self.base_url = base_url.rstrip("/")
        self.num_users = num_users
        self.duration = duration
        self.min_msg_len = min_msg_len
        self.max_msg_len = max_msg_len
        self.min_interval = min_interval
        self.max_interval = max_interval
        self.feed_ratio = feed_ratio
        self.output_dir = output_dir
        self.session = requests.Session()

        self.results_lock = threading.Lock()
        self.message_times = []
        self.feed_times = []
        self.message_timestamps = []
        self.feed_timestamps = []
        self.message_sizes = []
        self.user_activity = {}
        self.start_time = None
        self.end_time = None
        self.monitor_stop = None
        self.lb_metrics_timeline = []
        self.backend_stats_timeline = []
        self.lb_metrics_times = []
        self.backend_times = {}

    def monitor_stats(self, stop_event):
        while not stop_event.is_set():
            try:
                resp = self.session.get(f"{self.base_url}/lb/metrics", timeout=2)
                if resp.ok:
                    data = resp.json()
                    ts = time.monotonic() - self.start_time
                    with self.results_lock:
                        self.lb_metrics_timeline.append(data)
                        self.lb_metrics_times.append(ts)
                resp2 = self.session.get(f"{self.base_url}/lb/status", timeout=2)
                if resp2.ok:
                    data = resp2.json()
                    ts = time.monotonic() - self.start_time
                    with self.results_lock:
                        self.backend_stats_timeline.append(data)
                        for entry in data:
                            url = entry.get("url", "")
                            self.backend_times.setdefault(url, {"times": [], "in_flight": [], "load_score": []})
                            self.backend_times[url]["times"].append(ts)
                            self.backend_times[url]["in_flight"].append(entry.get("in_flight", 0))
                            self.backend_times[url]["load_score"].append(entry.get("load_score", 0))
            except Exception:
                pass
            stop_event.wait(2)

    def random_string(self, min_len, max_len):
        length = random.randint(min_len, max_len)
        return "".join(random.choices(string.ascii_letters + string.digits + " ", k=length))

    def send_message(self, user_id):
        client_name = f"user-{user_id}"
        msg = self.random_string(self.min_msg_len, self.max_msg_len)
        payload = {"client-name": client_name, "msg": msg}
        start = time.monotonic()
        try:
            resp = self.session.post(f"{self.base_url}/message", json=payload, timeout=10)
            elapsed = (time.monotonic() - start) * 1000
            with self.results_lock:
                self.message_times.append(elapsed)
                self.message_timestamps.append(time.monotonic() - self.start_time)
                self.message_sizes.append(len(msg))
                self.user_activity.setdefault(user_id, {"messages": 0, "errors": 0})
                self.user_activity[user_id]["messages"] += 1
                if resp.status_code >= 400:
                    self.user_activity[user_id]["errors"] += 1
            return True, resp.status_code, elapsed
        except Exception as e:
            elapsed = (time.monotonic() - start) * 1000
            with self.results_lock:
                self.message_times.append(elapsed)
                self.message_timestamps.append(time.monotonic() - self.start_time)
                self.message_sizes.append(len(msg))
                self.user_activity.setdefault(user_id, {"messages": 0, "errors": 0})
                self.user_activity[user_id]["errors"] += 1
            return False, str(e), elapsed

    def send_feed(self, user_id):
        start = time.monotonic()
        try:
            resp = self.session.get(f"{self.base_url}/feed", timeout=10)
            elapsed = (time.monotonic() - start) * 1000
            with self.results_lock:
                self.feed_times.append(elapsed)
                self.feed_timestamps.append(time.monotonic() - self.start_time)
            return True, resp.status_code, elapsed
        except Exception as e:
            elapsed = (time.monotonic() - start) * 1000
            with self.results_lock:
                self.feed_times.append(elapsed)
                self.feed_timestamps.append(time.monotonic() - self.start_time)
            return False, str(e), elapsed

    def user_loop(self, user_id, stop_event):
        request_count = 0
        while not stop_event.is_set():
            is_feed = (request_count % max(1, int(1 / self.feed_ratio)) == 0) if self.feed_ratio < 1 else False
            if is_feed:
                self.send_feed(user_id)
            else:
                self.send_message(user_id)
            request_count += 1
            interval = random.uniform(self.min_interval, self.max_interval)
            stop_event.wait(interval)

    def run(self):
        os.makedirs(self.output_dir, exist_ok=True)
        self.start_time = time.monotonic()
        stop_event = threading.Event()
        self.monitor_stop = threading.Event()

        print(f"Starting load test: {self.num_users} users, {self.duration}s duration")
        print(f"Message length: {self.min_msg_len}-{self.max_msg_len} chars")
        print(f"Interval: {self.min_interval}-{self.max_interval}s")
        print(f"Target: {self.base_url}")

        monitor_thread = threading.Thread(target=self.monitor_stats, args=(self.monitor_stop,), daemon=True)
        monitor_thread.start()

        with ThreadPoolExecutor(max_workers=self.num_users) as executor:
            futures = [
                executor.submit(self.user_loop, i, stop_event)
                for i in range(self.num_users)
            ]
            time.sleep(self.duration)
            stop_event.set()
            self.monitor_stop.set()
            for f in as_completed(futures):
                f.result()

        monitor_thread.join(timeout=3)
        self.end_time = time.monotonic()
        self.generate_plots()
        self.generate_summary()

    def generate_plots(self):
        plt.style.use("seaborn-v0_8-darkgrid")

        if self.message_times:
            fig, axes = plt.subplots(2, 2, figsize=(14, 10))
            fig.suptitle("Load Test Results", fontsize=16, fontweight="bold")

            ax1 = axes[0, 0]
            times = self.message_times[:500]
            ax1.plot(range(len(times)), times, linewidth=0.5, alpha=0.7, color="#FF4500")
            ax1.set_xlabel("Request #")
            ax1.set_ylabel("Response Time (ms)")
            ax1.set_title("Message POST Response Time")
            avg = sum(times) / len(times)
            ax1.axhline(y=avg, color="red", linestyle="--", label=f"Avg: {avg:.1f}ms")
            ax1.legend()

            ax2 = axes[0, 1]
            if self.feed_times:
                ft = self.feed_times[:500]
                ax2.plot(range(len(ft)), ft, linewidth=0.5, alpha=0.7, color="#4682B4")
                ax2.set_xlabel("Request #")
                ax2.set_ylabel("Response Time (ms)")
                ax2.set_title("Feed GET Response Time")
                favg = sum(ft) / len(ft)
                ax2.axhline(y=favg, color="red", linestyle="--", label=f"Avg: {favg:.1f}ms")
                ax2.legend()
            else:
                ax2.text(0.5, 0.5, "No feed requests", ha="center", va="center")
                ax2.set_title("Feed GET Response Time")

            ax3 = axes[1, 0]
            ax3.hist(self.message_times, bins=50, color="#FF4500", alpha=0.7, edgecolor="black", linewidth=0.5)
            ax3.set_xlabel("Response Time (ms)")
            ax3.set_ylabel("Frequency")
            ax3.set_title("Message POST Response Time Distribution")
            ax3.axvline(x=avg, color="red", linestyle="--", label="Mean")
            ax3.legend()

            ax4 = axes[1, 1]
            if self.message_timestamps:
                bin_size = 1.0
                total_dur = self.message_timestamps[-1] if self.message_timestamps else 1
                bins = int(total_dur / bin_size) + 1
                hist = [0] * bins
                for t in self.message_timestamps:
                    idx = min(int(t / bin_size), bins - 1)
                    hist[idx] += 1
                ax4.bar(range(len(hist)), hist, color="#FF4500", alpha=0.7, width=0.8)
                ax4.set_xlabel("Time (seconds)")
                ax4.set_ylabel("Requests / sec")
                ax4.set_title("Throughput (Message POST)")

            plt.tight_layout()
            plot_path = os.path.join(self.output_dir, f"load_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
            plt.savefig(plot_path, dpi=150, bbox_inches="tight")
            plt.close()
            print(f"Saved plot: {plot_path}")

        if self.message_sizes:
            fig, ax = plt.subplots(figsize=(8, 5))
            ax.hist(self.message_sizes, bins=40, color="#FF4500", alpha=0.7, edgecolor="black", linewidth=0.5)
            ax.set_xlabel("Message Size (characters)")
            ax.set_ylabel("Frequency")
            ax.set_title("Message Size Distribution")
            plot_path = os.path.join(self.output_dir, f"message_sizes_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
            plt.savefig(plot_path, dpi=150, bbox_inches="tight")
            plt.close()
            print(f"Saved plot: {plot_path}")

        if self.user_activity:
            fig, ax = plt.subplots(figsize=(10, 5))
            user_ids = sorted(self.user_activity.keys())
            messages = [self.user_activity[u]["messages"] for u in user_ids]
            errors = [self.user_activity[u]["errors"] for u in user_ids]
            x = range(len(user_ids))
            ax.bar(x, messages, color="#FF4500", alpha=0.7, label="Messages")
            ax.bar(x, errors, bottom=messages, color="red", alpha=0.7, label="Errors")
            ax.set_xlabel("User ID")
            ax.set_ylabel("Request Count")
            ax.set_title("Per-User Activity")
            ax.set_xticks(x)
            ax.legend()
            plot_path = os.path.join(self.output_dir, f"user_activity_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
            plt.savefig(plot_path, dpi=150, bbox_inches="tight")
            plt.close()
            print(f"Saved plot: {plot_path}")

        if self.message_timestamps and len(self.message_timestamps) > 1:
            fig, ax = plt.subplots(figsize=(10, 5))
            bin_size = 2.0
            total_dur = self.message_timestamps[-1]
            bins = int(total_dur / bin_size) + 1
            hist = [0] * bins
            for t in self.message_timestamps:
                idx = min(int(t / bin_size), bins - 1)
                hist[idx] += 1
            times_axis = [i * bin_size for i in range(len(hist))]
            ax.plot(times_axis, hist, linewidth=2, color="#FF4500")
            ax.fill_between(times_axis, hist, alpha=0.3, color="#FF4500")
            ax.set_xlabel("Time (seconds)")
            ax.set_ylabel("Requests / second")
            ax.set_title("System Utilization - Total RPS")
            plot_path = os.path.join(self.output_dir, f"utilization_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
            plt.savefig(plot_path, dpi=150, bbox_inches="tight")
            plt.close()
            print(f"Saved plot: {plot_path}")

        if self.backend_stats_timeline:
            fig, ax = plt.subplots(figsize=(12, 6))
            for url, data in self.backend_times.items():
                if data["times"]:
                    label = url.split("/")[-1].split(":")[-1]
                    ax.plot(data["times"], data["in_flight"], label=f"{label} (in-flight)", linewidth=1.5)
            ax.set_xlabel("Time (seconds)")
            ax.set_ylabel("In-Flight Requests")
            ax.set_title("Per-Backend System Utilization (In-Flight Requests)")
            ax.legend()
            plot_path = os.path.join(self.output_dir, f"backend_utilization_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
            plt.savefig(plot_path, dpi=150, bbox_inches="tight")
            plt.close()
            print(f"Saved plot: {plot_path}")

        if self.lb_metrics_timeline:
            fig, ax = plt.subplots(figsize=(12, 6))
            total_times = [t for t, m in zip(self.lb_metrics_times, self.lb_metrics_timeline)]
            total_vals = [m.get("total", 0) for m in self.lb_metrics_timeline]
            succ_vals = [m.get("success", 0) for m in self.lb_metrics_timeline]
            fail_vals = [m.get("failed", 0) for m in self.lb_metrics_timeline]
            err_vals = [m.get("backend_errors", 0) for m in self.lb_metrics_timeline]
            ax.plot(total_times, total_vals, label="Total", linewidth=2, color="#333333")
            ax.plot(total_times, succ_vals, label="Success", linewidth=1.5, color="#4CAF50")
            ax.plot(total_times, fail_vals, label="Failed", linewidth=1.5, color="#F44336")
            ax.plot(total_times, err_vals, label="Backend Errors", linewidth=1.5, color="#FF9800")
            ax.set_xlabel("Time (seconds)")
            ax.set_ylabel("Cumulative Count")
            ax.set_title("Load Balancer Metrics Over Time")
            ax.legend()
            plot_path = os.path.join(self.output_dir, f"lb_metrics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
            plt.savefig(plot_path, dpi=150, bbox_inches="tight")
            plt.close()
            print(f"Saved plot: {plot_path}")

    def generate_summary(self):
        msg_rt = self.message_times
        feed_rt = self.feed_times
        summary = {
            "config": {
                "base_url": self.base_url,
                "num_users": self.num_users,
                "duration_sec": self.duration,
                "min_msg_len": self.min_msg_len,
                "max_msg_len": self.max_msg_len,
                "min_interval_sec": self.min_interval,
                "max_interval_sec": self.max_interval,
            },
            "results": {
                "total_message_requests": len(msg_rt),
                "total_feed_requests": len(feed_rt),
                "total_errors": sum(u["errors"] for u in self.user_activity.values()),
                "duration_sec": round(self.end_time - self.start_time, 2) if self.end_time else 0,
            },
            "message_stats": self._rt_stats(msg_rt),
            "feed_stats": self._rt_stats(feed_rt),
            "per_user": {f"user_{u}": v for u, v in sorted(self.user_activity.items())},
        }

        summary_path = os.path.join(self.output_dir, f"summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        with open(summary_path, "w") as f:
            json.dump(summary, f, indent=2)
        print(f"Saved summary: {summary_path}")

        print("\n=== LOAD TEST SUMMARY ===")
        print(f"Duration: {summary['results']['duration_sec']}s")
        print(f"Message requests: {summary['results']['total_message_requests']}")
        print(f"Feed requests: {summary['results']['total_feed_requests']}")
        print(f"Errors: {summary['results']['total_errors']}")
        if summary["message_stats"]["avg_response_ms"]:
            print(f"Message avg RT: {summary['message_stats']['avg_response_ms']}ms")
            print(f"Message p50 RT: {summary['message_stats']['p50_response_ms']}ms")
            print(f"Message p95 RT: {summary['message_stats']['p95_response_ms']}ms")
            print(f"Message p99 RT: {summary['message_stats']['p99_response_ms']}ms")
        if summary["feed_stats"]["avg_response_ms"]:
            print(f"Feed avg RT: {summary['feed_stats']['avg_response_ms']}ms")
        print("=========================\n")

    def _rt_stats(self, times):
        if not times:
            return {"avg_response_ms": 0, "min_response_ms": 0, "max_response_ms": 0,
                    "p50_response_ms": 0, "p95_response_ms": 0, "p99_response_ms": 0}
        s = sorted(times)
        n = len(s)
        return {
            "avg_response_ms": round(sum(times) / n, 2),
            "min_response_ms": round(min(times), 2),
            "max_response_ms": round(max(times), 2),
            "p50_response_ms": round(s[n // 2], 2),
            "p95_response_ms": round(s[min(int(n * 0.95), n - 1)], 2),
            "p99_response_ms": round(s[min(int(n * 0.99), n - 1)], 2),
        }


def main():
    parser = argparse.ArgumentParser(description="Load Generator for Load Balancer Testing")
    parser.add_argument("-u", "--users", type=int, default=10, help="Number of concurrent users")
    parser.add_argument("-d", "--duration", type=int, default=60, help="Duration in seconds")
    parser.add_argument("-m", "--min-msg-len", type=int, default=10, help="Minimum message length")
    parser.add_argument("-M", "--max-msg-len", type=int, default=200, help="Maximum message length")
    parser.add_argument("-i", "--min-interval", type=float, default=0.5, help="Minimum interval between messages (seconds)")
    parser.add_argument("-I", "--max-interval", type=float, default=2.0, help="Maximum interval between messages (seconds)")
    parser.add_argument("-U", "--url", default="http://localhost:3000", help="Load balancer URL")
    parser.add_argument("-o", "--output", default="./output", help="Output directory for plots")
    parser.add_argument("--feed-ratio", type=float, default=0.2, help="Ratio of feed requests (0.0-1.0)")

    args = parser.parse_args()

    gen = LoadGenerator(
        base_url=args.url,
        num_users=args.users,
        duration=args.duration,
        min_msg_len=args.min_msg_len,
        max_msg_len=args.max_msg_len,
        min_interval=args.min_interval,
        max_interval=args.max_interval,
        feed_ratio=args.feed_ratio,
        output_dir=args.output,
    )
    gen.run()


if __name__ == "__main__":
    main()
