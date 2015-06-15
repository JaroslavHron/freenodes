/**
 * Author: Jaroslav Hron <jaroslav.hron@mff.cuni.cz>
 * Date: May 28, 2015
 * Version: 1.0
 * License: use freely for any purpose
 * Copyright: none
 **/

/**
 * Notes: needs a lot of code cleanup....
 **/

import std.stdio;
import std.string;
import std.process;
import std.array;
import std.conv;
import std.getopt;
import std.regex;
import core.time;

// color support
enum Color {
  fgBlack = 30, fgRed, fgGreen, fgYellow, fgBlue, fgMagenta, fgCyan, fgWhite,
  bgBlack = 40, bgRed, bgGreen, bgYellow, bgBlue, bgMagenta, bgCyan, bgWhite
}

string color(string text, Color c) {
  return "\033[" ~ c.to!int.to!string ~ "m" ~ text ~ "\033[0m";
}

 
struct Node { 
  string name;
  int sockets; 
  int cores_per_socket;
  int threads_per_core;
  int cpus;
  int cpu_alloc;
  int cores;
  string state;
  string sload;
  float load;
  string[] jobs;
}

struct Job { 
  int id;
  string name;
  string partition;
  string user;
  string run_time;
  string end_time;
  string time;
  string state;
  int nodes;
  int tasks;
  int cpus_per_task;
  int ncpus;
  int[][string] cpus;
  string[] allocations;
}

auto scontrol_expand_cpuids(string cpuids)
{
  int[] np;
  auto result=cpuids.strip().split(",");
  foreach(r; result) 
    {
      auto m=r.split("-");
      assert(m.length==1||m.length==2);
      if(m.length==2) 
        {
          for(auto i=to!int(m[0]); i<=to!int(m[1]); i++)
            np~=i;
        }
      if(m.length==1)
        {
          np~=to!int(m[0]);
        }
    }
  return(np);
}

auto scontrol_count_cpuids(string cpuids)
{
  int np=0;
  auto result=cpuids.strip().split(",");
  foreach(r; result) 
    {
      auto m=r.split("-");
      assert(m.length==1||m.length==2);
      if(m.length==2) np+=1+to!int(m[1])-to!int(m[0]); 
      if(m.length==1) np+=1;
    }
  return(np);
}

auto scontrol_expand_hosts(string node_list)
{
  auto cmd=format("scontrol -a -o -d show hostname %s",node_list);
  auto result=executeShell(cmd).output.strip().split("\n");
  return(result);
}

auto scontrol_jobs_info()
{
  auto cmd=format("scontrol -a -o -d show job");
  auto result=executeShell(cmd).output.strip().split("\n");

  Job[string] jobs;

  foreach(int i, string l; result)
    {
      auto id=matchFirst(l, r"(JobId)=([^ ]*)").captures[2];
      auto j=Job();
      j.name=id;
      j.id=to!int(id);
      j.state=matchFirst(l, r" (JobState)=([^ ]*)").captures[2];
      j.partition=matchFirst(l, r" (Partition)=([^ ]*)").captures[2];
      j.user=matchFirst(l, r" (Account)=([^ ]*)").captures[2];

      j.run_time=matchFirst(l, r" (RunTime)=([^ ]*)").captures[2];
      j.end_time=matchFirst(l, r" (TimeLimit)=([^ ]*)").captures[2];
      
      
      int dd=0;
      auto tt=j.end_time.split("-");
      if(tt.length>1) {dd=to!int(tt[0]); tt[0]=tt[1];}
      auto tt0=tt[0].split(":");
      auto dur1=days(dd)+hours(to!int(tt0[0]))+minutes(to!int(tt0[1]))+seconds(to!int(tt0[2]));
      tt=j.run_time.split("-");
      dd=0;
      if(tt.length>1) {dd=to!int(tt[0]); tt[0]=tt[1];}
      tt0=tt[0].split(":");
      auto dur2=days(dd)+hours(to!int(tt0[0]))+minutes(to!int(tt0[1]))+seconds(to!int(tt0[2]));
      auto dur0=dur1-dur2;
      auto s = dur0.split!("days", "hours", "minutes", "seconds")();
      j.time=format("%d-%02d:%02d:%02d",s.days,s.hours,s.minutes,s.seconds);

      j.nodes=matchFirst(l, r" (NumNodes)=([^ ]*)").captures[2].to!int;
      j.ncpus=matchFirst(l, r" (NumCPUs)=([^ ]*)").captures[2].to!int;
      j.cpus_per_task=matchFirst(l, r" (CPUs/Task)=([^ ]*)").captures[2].to!int;
      auto nl=matchFirst(l, r" (NodeList)=([^ ]*)").captures[2];
      auto nlex=scontrol_expand_hosts(nl);
      
      string[] nl2;
      int np=0;

      foreach(c; matchAll(l, r" (Nodes)=([^ ]*) (CPU_IDs)=([^ ]*)")) {
        auto tmpn=scontrol_expand_hosts(c.captures[2]);
        auto tmpnp=scontrol_expand_cpuids(c.captures[4]);
        foreach(k;tmpn) {np+=tmpnp.length; j.cpus[k]=tmpnp;}
        nl2 ~= tmpn;
      }
      j.tasks=np;
      j.allocations=nl2;

      jobs[id]=j;
    }
  return(jobs);
}

auto scontrol_nodes_info()
{
  auto cmd=format("scontrol -a -o -d show node");
  auto result=executeShell(cmd).output.strip().split("\n");

  Node[string] nodes;

  foreach(int i, string l; result)
    {
      auto id=match(l, r"(NodeName)=([^ ]*)").captures[2];
      auto n=Node();
      n.name=id;
      n.sockets=match(l, r"(Sockets)=([^ ]*)").captures[2].to!int;
      n.cores_per_socket=match(l, r"(CoresPerSocket)=([^ ]*)").captures[2].to!int;
      n.threads_per_core=match(l, r"(ThreadsPerCore)=([^ ]*)").captures[2].to!int;
      n.cpus=match(l, r"(CPUTot)=([^ ]*)").captures[2].to!int;
      n.cpu_alloc=match(l, r"(CPUAlloc)=([^ ]*)").captures[2].to!int;
      n.sload=match(l, r"(CPULoad)=([^ ]*)").captures[2];
      try n.load=n.sload.to!float;
      catch (ConvException) n.load=-1.0;
      n.state=match(l, r"(State)=([^ ]*)").captures[2];
      n.cores=n.sockets*n.cores_per_socket;
      nodes[id]=n;
    }
  return(nodes);
}





bool display_user=false;
bool display_time=false;
bool display_id=false;

string ids=".x!!!!!!-";
//string ids="\u25AE"~"\u25AF"~"\u25AF";


void main(string[] args)
{

  auto helpInformation = getopt(args, std.getopt.config.passThrough, std.getopt.config.bundling,
                                "id|i", "Display the job id", &display_id,
                                "time|t", "Display the remaining time of the job allocation", &display_time,
                                "user|u", "Display the user names", &display_user);

  auto nodes=executeShell("nodeattr -s ubuntu-14.04");
  if (nodes.status != 0) writeln("Failed to retrieve nodes listing");
  auto node_array=nodes.output.split();

  Color[string] part_color;
  part_color["express"]=Color.fgRed;
  part_color["short"]=Color.fgBlue;
  part_color["long"]=Color.fgGreen;
  part_color["other"]=Color.fgYellow;
  part_color["free"]=Color.fgCyan;
  part_color["down"]=Color.fgMagenta;

  string[string] status_name;
  status_name["ALLOCATED"]="full";
  status_name["IDLE"]="free";
  status_name["MIXED"]="part";
  status_name["DOWN"]="down";

  if (helpInformation.helpWanted)
    {
      defaultGetoptPrinter("List cluster occupation info from SLURM.",
                           helpInformation.options);
      return;
    }

  auto head="";
  if(display_user||display_time||display_id) {
    head=" [";
    if(display_id)   head~="ID|";
    if(display_user) head~="owner|";
    if(display_time) head~="remaining time|";
    head~="cores]";
  }

  writeln("node busy cores state   load allocated cores in ", "express".color(part_color["express"]),"/", "short".color(part_color["short"]),"/", "long".color(part_color["long"]),"/", "other".color(part_color["other"]),head);

  auto allnodes=scontrol_nodes_info();
  auto alljobs=scontrol_jobs_info();

  foreach ( j ; alljobs) 
    {
      foreach( k ; j.allocations ) allnodes[k].jobs ~= j.name ;
    }

  bool print_mark=false;

  foreach (int i, string node_name; node_array)
    {

      auto node=allnodes[node_name];
      auto load=format("%6s",node.sload);

      string mark=" ";
      if (node.load>0.2 && node.state=="IDLE") mark="!";
      if (node.load>node.threads_per_core*node.cores+0.2) mark="!";
      if (mark!=" ") print_mark=true;

      /*
      if (node.load<0.0) load=color(load,Color.fgBlue);
      if (node.load<0.2) load=color(load,Color.fgGreen);
      else if (node.load<node.cores) load=color(load,Color.fgYellow);
      else load=color(load,Color.fgRed);
      */
      
      writef("%1s%3s (%2d of %2d) %5s %s ",mark, node.name,node.cpu_alloc/node.threads_per_core, node.cores, status_name[node.state],load);

      auto sum=0;
      int[] map;
      char[] smap;
      Color[] cmap;

      map.length=node.cores;
      smap.length=node.cores;
      cmap.length=node.cores;
      writef("|");

      for(auto k=0; k<node.cores; k++) {map[k]=0; smap[k]=ids[0]; cmap[k]=part_color["free"];}

      foreach ( j; node.jobs)
        {
          auto job=alljobs[j];
          if(job.state=="RUNNING") {
            for(auto k=0; k<job.cpus[node.name].length ; k++) {
              auto cpuid=job.cpus[node.name][k];
              map[cpuid] +=1 ;
              if(node.state=="DOWN") {
                smap[cpuid] = ids[8];
                cmap[cpuid]=part_color.get(job.partition,part_color["down"]);
              } else {
                smap[cpuid] = ids[map[cpuid]];
                cmap[cpuid]=part_color.get(job.partition,part_color["other"]);
              }
              sum+=1;
            }
          }
        }

      for( auto k=0; k<node.cores; k++) 
        {
          writef("%s".color(cmap[k]),smap[k]);
        }

      writef("|");
      writef(" ");

      if(display_user || display_time || display_id)
        {
          foreach ( j; node.jobs)
            {
              auto job=alljobs[j];
              if(job.state=="RUNNING") {
              string id="";
              if (display_id) id~=format("%s|",job.id);
              if (display_user) id~=format("%s|",job.user);
              if (display_time) id~=format("%s|",job.time);
              writef("[");
              writef("%s".color(part_color.get(job.partition,part_color["other"])),id);
              writef("%d".color(part_color.get(job.partition,part_color["other"])),job.cpus[node.name].length);
              writef("] ");
              }
            }
        }
      writeln("");
    }
  if(print_mark) writeln("Notes: !-marked nodes are overcommited or busy with job outside the slurm control.");
}

