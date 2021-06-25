/*
Copyright 2021 The Kubernetes Authors.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package ui

import (
	"context"
	"fmt"
	"time"

	"github.com/charmbracelet/bubbles/paginator"
	"github.com/charmbracelet/bubbles/progress"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/muesli/reflow/indent"
	"github.com/muesli/reflow/wordwrap"
	"github.com/muesli/reflow/wrap"
	clusterv1 "sigs.k8s.io/cluster-api/api/v1alpha3"
)

const (
	fps          = 60
	padding      = 2
	maxWidth     = 80
	errorColor   = lipgloss.Color("#dc322f")
	headingColor = lipgloss.Color("#268bd2")
	infoColor    = lipgloss.Color("#859900")
)

type Tool interface {
	CalculatePercentage() float64
	GetClusters(context.Context) ([]*clusterv1.Cluster, error)
	GetOutputFor(*clusterv1.Cluster) string
	GetErrorFor(*clusterv1.Cluster) error
	Run(context.Context)
	CheckPrerequisites(context.Context) error
}

func NewModel(title string, tool Tool, tuiEnabled bool) (Model, error) {
	prog, err := progress.NewModel(progress.WithScaledGradient("#FF7CCB", "#FDFF8C"))
	if err != nil {
		return Model{}, fmt.Errorf("failed to generate progress bar: %w", err)
	}

	p := paginator.NewModel()

	return Model{ //nolint:exhaustivestruct
		title:      title,
		Tool:       tool,
		progress:   prog,
		paginator:  p,
		tuiEnabled: tuiEnabled,
		viewport:   viewport.Model{}, //nolint:exhaustivestruct
	}, nil
}

type Model struct {
	title           string
	Tool            Tool
	progress        *progress.Model
	paginator       paginator.Model
	viewPortCluster string
	viewport        viewport.Model
	percent         float64
	err             error
	height          int
	width           int
	finished        bool
	tuiEnabled      bool
}

type progressTick time.Time

type finishedMsg string

func progressTickCmd() tea.Cmd {
	return tea.Tick(time.Second/fps, func(t time.Time) tea.Msg {
		return progressTick(t)
	})
}

func (m *Model) runTool() tea.Msg {
	m.Tool.Run(context.TODO())

	return finishedMsg("")
}

func (m *Model) checkPrerequisites() tea.Msg {
	return m.Tool.CheckPrerequisites(context.TODO()) //nolint: wrapcheck
}

func (m Model) Init() tea.Cmd {
	return tea.Batch(tea.Sequentially(m.checkPrerequisites, m.runTool), progressTickCmd())
}

func (m *Model) updateViewport() {
	outputHeadings := lipgloss.NewStyle().Foreground(headingColor)
	clusterOutputStyle := lipgloss.NewStyle().Foreground(infoColor)
	clusterErrorStyle := lipgloss.NewStyle().Foreground(errorColor)

	body := ""

	clusters, err := m.Tool.GetClusters(context.TODO())
	if err != nil {
		body += lipgloss.NewStyle().Foreground(errorColor).Render(fmt.Sprintf("Error: %s", err.Error())) + "\n"
	}

	start, end := m.paginator.GetSliceBounds(len(clusters))

	for _, c := range clusters[start:end] {
		m.viewPortCluster = fmt.Sprintf("%s/%s", c.Namespace, c.Name)

		out := m.Tool.GetOutputFor(c)

		if len(out) > 0 {
			body += indent.String(outputHeadings.Render("Output:"), 4) + "\n"

			out = wrap.String(wordwrap.String(out, m.viewport.Width-8), m.viewport.Width-8) //nolint:gomnd
			out = indent.String(out, 8)
			body += clusterOutputStyle.Render(out) + "\n"
		}

		if err := m.Tool.GetErrorFor(c); err != nil {
			errOut := wrap.String(wordwrap.String(err.Error(), m.viewport.Width-4), m.viewport.Width-4) //nolint:gomnd
			errOut = indent.String(errOut, 4)
			body += clusterErrorStyle.Render(fmt.Sprintf("Error: %s", errOut)) + "\n"
		}
	}

	m.viewport.SetContent(body)
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) { //nolint:cyclop
	var cmd tea.Cmd

	switch msg := msg.(type) {
	case finishedMsg:
		if !m.tuiEnabled {
			// if the TUI is disabled, we want to quit immediately instead of waiting
			// for the user to exit
			cmd = tea.Quit
		}

		m.finished = true
	case error:
		m.err = msg

		if m.err != nil {
			m.finished = true
		}
	case tea.WindowSizeMsg:
		footerHeight := 1
		headerHeight := 4
		navHeight := 6
		viewportHeadingHeight := 1
		viewPortHeightPadding := footerHeight + headerHeight + navHeight + viewportHeadingHeight + padding*2 //nolint: gomnd

		m.width = msg.Width
		m.height = msg.Height
		m.viewport.Width = msg.Width - padding
		m.viewport.Height = msg.Height - viewPortHeightPadding
		m.progress.Width = msg.Width - padding*2 - 4 //nolint: gomnd

		if m.progress.Width > maxWidth {
			m.progress.Width = maxWidth
		}

		return m, nil
	case progressTick:
		m.updateTick()

		cmd = progressTickCmd()
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c":
			cmd = tea.Quit

		case "q", "esc":
			if m.finished {
				cmd = tea.Quit
			}

		default:
			m.updatePaginatorAndViewPort(msg)
		}
	}

	m.updateViewport()

	return m, cmd
}

func (m *Model) updateTick() {
	m.percent = m.Tool.CalculatePercentage()
	clusters, _ := m.Tool.GetClusters(context.TODO())
	m.paginator.SetTotalPages(len(clusters))
}

func (m *Model) updatePaginatorAndViewPort(msg tea.KeyMsg) { //nolint:cyclop
	pageBefore := m.paginator.Page //nolint:ifshort

	// previous page: a, left, shift+tab
	// next page: d, right, tab
	// up one line: w, up
	// down one line: s, down
	// up one page: PageUp
	// down one page: PageDown
	// go to top: Home
	// go to end: End

	switch msg.Type { //nolint: exhaustive
	case tea.KeyTab, tea.KeyRight:
		m.paginator.NextPage()
	case tea.KeyShiftTab, tea.KeyLeft:
		m.paginator.PrevPage()
	case tea.KeyHome:
		m.viewport.GotoTop()
	case tea.KeyEnd:
		m.viewport.GotoBottom()
	case tea.KeyPgUp:
		m.viewport.ViewUp()
	case tea.KeyPgDown:
		m.viewport.ViewDown()
	case tea.KeyUp:
		m.viewport.LineUp(1)
	case tea.KeyDown:
		m.viewport.LineDown(1)
	case tea.KeyRunes:
		switch string(msg.Runes) {
		case "w":
			m.viewport.LineUp(1)
		case "a":
			m.paginator.PrevPage()
		case "s":
			m.viewport.LineDown(1)
		case "d":
			m.paginator.NextPage()
		}
	}

	// If we've changed pages, update the viewport and reset the view to the end
	if m.paginator.Page != pageBefore {
		m.updateViewport()
		m.viewport.GotoBottom()
	}
}

func (m Model) View() string {
	header := fmt.Sprintf("%s\n\n%s\n", m.title, m.progress.View(m.percent))
	header = lipgloss.PlaceHorizontal(m.width-padding, lipgloss.Center, header)

	footer := ""

	if m.finished {
		footer += "Press 'q' to quit"
	}

	footer = lipgloss.PlaceHorizontal(m.width-padding, lipgloss.Center, footer)

	headerHeight := lipgloss.Height(header)
	footerHeight := lipgloss.Height(footer)
	bodyHeight := m.height - headerHeight - footerHeight - (padding * 2) //nolint: gomnd

	body := ""
	// If we encountered an error with pre-requisites the error output should override
	// any other output
	if m.err != nil {
		errOutput := fmt.Sprintf("Error: %s", m.err.Error())
		errOutput = lipgloss.NewStyle().Foreground(errorColor).Render(errOutput) + "\n"
		body += wrap.String(wordwrap.String(errOutput, m.width-padding), m.width-padding)
		body = lipgloss.Place(m.width-padding, bodyHeight, lipgloss.Left, lipgloss.Top, body)
	} else {
		scrollPercent := fmt.Sprintf("%3.f%%", m.viewport.ScrollPercent()*100) //nolint:gomnd
		// Only show scroll percentage if everything doesn't fit on a single page
		if m.viewport.AtBottom() && m.viewport.AtTop() {
			scrollPercent = ""
		}

		paginator := "cluster: " + m.paginator.View()

		paginatorNav := "<-/-> to view output for additional workload clusters"
		viewportNav := "PageUp/PageDown to scroll through output\n"

		nav := lipgloss.JoinVertical(lipgloss.Center, scrollPercent, paginator, paginatorNav, viewportNav)
		nav = lipgloss.PlaceHorizontal(m.width-padding, lipgloss.Center, nav)

		clusterHeading := ""

		if m.viewPortCluster != "" {
			clusterHeading = lipgloss.NewStyle().Foreground(headingColor).Render("Cluster: " + m.viewPortCluster)
		}

		body += lipgloss.JoinVertical(lipgloss.Left, clusterHeading, m.viewport.View(), nav)
		body = lipgloss.Place(m.width-padding, bodyHeight, lipgloss.Left, lipgloss.Bottom, body)
	}

	body = lipgloss.NewStyle().Border(lipgloss.NormalBorder(), true, false).Render(body)
	output := lipgloss.JoinVertical(lipgloss.Center, header, body, footer)
	output = lipgloss.NewStyle().BorderStyle(lipgloss.RoundedBorder()).Render(output)

	return output
}
